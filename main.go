package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const topic string = "mynewtopic"

type UserData struct {
	Name         string `json:"name"`
	Age          int    `json:"age"`
	MirageStatus bool   `json:"mirage_status"`
}

func WriteKafka(acl plain.Mechanism) {
	w := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers: []string{"localhost:9092"},
			Dialer: &kafka.Dialer{
				SASLMechanism: acl,
			},
			Topic: topic,
		},
	)
	w.AllowAutoTopicCreation = true
	datas := []UserData{
		{"Lwin Mg Mg", 20, true},
		{"Nadi Yar Hphone", 21, true},
		{"No one", 10, false},
	}
	for _, v := range datas {
		jsonData, err := json.Marshal(&v)
		if err != nil {
			fmt.Printf("Error on marshal mesg : %v", err)
		}
		outputB := make([]byte, 1000)
		base64.StdEncoding.Encode(outputB, jsonData)
		err = w.WriteMessages(
			context.Background(),
			kafka.Message{
				Key:   outputB,
				Value: jsonData,
			},
		)

		if err != nil {
			fmt.Printf("Error on writing mesg : %v", err)
		}
	}
}

func ReadKafka(acl plain.Mechanism) {
	r := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{"localhost:9092"},
			GroupID: "postman",
			Dialer: &kafka.Dialer{
				SASLMechanism: acl,
			},
			Topic: topic,
		},
	)
	ctx := context.Background()
	ctxTO, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	for {
		mesg, err := r.FetchMessage(ctxTO)
		if err != nil {
			fmt.Printf("Error on reading mesg : %v", err)
			break
		}
		fmt.Println(string(mesg.Value))
		fmt.Println("***************************************************************")
		r.CommitMessages(context.Background(), mesg)
	}

}

func main() {
	fmt.Println("Lwin Mg Mg")
	acl := plain.Mechanism{
		Username: "admin",
		Password: "admin-secret",
	}
	// WriteKafka(acl)
	ReadKafka(acl)
}
