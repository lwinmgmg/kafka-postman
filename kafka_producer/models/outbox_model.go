package models

import "gorm.io/gorm"

type OutBox struct {
	gorm.Model
	Key   []byte
	Value []byte
}

func (outbox *OutBox) TableName() string {
	return "outbox"
}
