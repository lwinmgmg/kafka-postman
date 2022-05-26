package models_test

import (
	"os"
	"testing"

	_ "github.com/lwinmgmg/kafka-postman/kafka_producer/models"
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	os.Exit(exitCode)
}
