package models

import "time"

const (
	DRAFT int8 = iota + 1
	QUEUE
	DONE
	FAILED
)

type OutBox struct {
	ID           uint `gorm:"primarykey"`
	CreatedAt    time.Time
	UpdatedAt    time.Time
	State        int8
	Topic        string
	Header       string
	Key          string
	Value        string
	FailedReason string
}

func (outbox *OutBox) TableName() string {
	return *Prefix + "_" + "outbox"
}
