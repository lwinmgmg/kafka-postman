package models

import (
	"github.com/lwinmgmg/kafka-postman/dbm"
	"gorm.io/gorm"
)

var db *gorm.DB

func init() {
	db = dbm.GetDB()
	db.AutoMigrate(&OutBox{})
}
