package kafkamgr

import (
	"context"
	"time"

	"github.com/lwinmgmg/kafka-postman/environ"
	"github.com/lwinmgmg/kafka-postman/logmgr"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var (
	env    *environ.Environ
	logger = logmgr.GetLogger()
)

// func init() {
// 	env = environ.GetAllEnvSettings()
// 	ConnList = make(map[string]*kafka.Writer, 10)
// 	acl = &plain.Mechanism{
// 		Username: "admin",
// 		Password: "admin-secret",
// 	}
// }

type KafkaServer struct {
	ConnTopicMap map[string]*kafka.Writer
	Brokers      []string
	RetryCount        int
	Acl          *plain.Mechanism
}

func NewKafkaServer(brokerList []string, retryCount int, acl *plain.Mechanism) *KafkaServer {
	return &KafkaServer{
		ConnTopicMap: make(map[string]*kafka.Writer),
		Brokers:      brokerList,
		RetryCount:        retryCount,
		Acl:          acl,
	}
}

func (ks *KafkaServer) Produce(topic, key, value string, headers ...protocol.Header) error {
	var writer *kafka.Writer
	var ok bool = false
	if writer, ok = ks.ConnTopicMap[topic]; !ok {
		writer = kafka.NewWriter(
			kafka.WriterConfig{
				Brokers: []string{"localhost:9092"},
				Dialer: &kafka.Dialer{
					SASLMechanism: *ks.Acl,
				},
				Topic: topic,
			},
		)
		ks.ConnTopicMap[topic] = writer
	}
	var err error
	for i := 0; i < ks.RetryCount; i++ {
		ctx := context.Background()
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		err = writer.WriteMessages(
			timeoutCtx,
			kafka.Message{
				Key:     []byte(key),
				Value:   []byte(value),
				Headers: headers,
			},
		)
		if err != nil {
			logger.Error("Error on producing message : %v", err)
			continue
		}
		break
	}
	return err
}

// func WriteKafka(acl plain.Mechanism) {
// 	w := kafka.NewWriter(
// 		kafka.WriterConfig{
// 			Brokers: []string{"localhost:9092"},
// 			Dialer: &kafka.Dialer{
// 				SASLMechanism: acl,
// 			},
// 			Topic: topic,
// 		},
// 	)
// 	w.AllowAutoTopicCreation = true
// 	datas := []UserData{
// 		{"Lwin Mg Mg", 20, true},
// 		{"Nadi Yar Hphone", 21, true},
// 		{"No one", 10, false},
// 	}
// 	for _, v := range datas {
// 		jsonData, err := json.Marshal(&v)
// 		if err != nil {
// 			fmt.Printf("Error on marshal mesg : %v", err)
// 		}
// 		outputB := make([]byte, 1000)
// 		base64.StdEncoding.Encode(outputB, jsonData)
// 		err = w.WriteMessages(
// 			context.Background(),
// 			kafka.Message{
// 				Key:   outputB,
// 				Value: jsonData,
// 			},
// 		)

// 		if err != nil {
// 			fmt.Printf("Error on writing mesg : %v", err)
// 		}
// 	}
// }
