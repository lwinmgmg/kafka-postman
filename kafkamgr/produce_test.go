package kafkamgr_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/lwinmgmg/kafka-postman/kafkamgr"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestProduce(t *testing.T) {
	dataMap := map[string]interface{}{
		"name": "Lwin Maung Maung",
		"age":  20,
	}
	dataMapByte, err := json.Marshal(dataMap)
	if err != nil {
		t.Error(err)
	}
	kafkaServer := kafkamgr.NewKafkaServer([]string{"localhost:9092"}, 5, &plain.Mechanism{Username: "admin", Password: "admin-secret"})
	if err := kafkaServer.Produce("test_topic", "user", string(dataMapByte)); err != nil {
		t.Errorf("Error on produce message : %v", err)
	}
}
