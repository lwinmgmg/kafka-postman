package models

import (
	"fmt"

	"github.com/lwinmgmg/kafka-postman/dbm"
	"github.com/lwinmgmg/kafka-postman/environ"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	db     *gorm.DB
	env    *environ.Environ
	Prefix *string
)

type UpdateFunc func(*gorm.DB) error

func init() {
	db = dbm.GetDB()
	env = environ.GetAllEnvSettings()
	Prefix = &env.TABLE_PREFIX
	db.AutoMigrate(&OutBox{})
}

type Table interface {
	TableName() string
}

type Manager struct {
	Table
}

func NewManager(table Table) *Manager {
	return &Manager{
		Table: table,
	}
}

func (mgr *Manager) Create(data any) error {
	return db.Model(mgr.Table).Create(data).Error
}

func (mgr *Manager) GetByID(id uint, dest any) error {
	return db.Model(mgr.Table).Take(dest, id).Error
}

func (mgr *Manager) GetByIDs(ids []uint, dest any) error {
	return db.Model(mgr.Table).Find(dest, ids).Error
}

func (mgr *Manager) GetByFilter(dest interface{}, cond string, args ...interface{}) error {
	return db.Model(mgr.Table).Where(cond, args...).Find(dest).Error
}

func (mgr *Manager) GetForUpdate(ids []uint, dest any, callBack UpdateFunc) (err error) {
	tx := db.Begin()
	defer func() {
		if rec := recover(); rec != nil {
			tx.Rollback()
			err = fmt.Errorf("Getting error in transaction : %v", rec)
		}
	}()
	if err := tx.Model(mgr.Table).Clauses(
		clause.Locking{
			Strength: "UPDATE",
			Options:  "NOWAIT",
		},
	).Find(dest, ids).Error; err != nil {
		return fmt.Errorf("Error on select locking : %w", err)
	}
	if err := callBack(tx); err != nil {
		return fmt.Errorf("Error on callback : %w", err)
	}
	tx.Commit()
	return nil
}
