package kafka_mgr

import (
	"github.com/lwinmgmg/kafka-postman/environ"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var (
	ConnList map[string]*kafka.Writer
	env      *environ.Environ
	acl      *plain.Mechanism
)

func init() {
	env = environ.GetAllEnvSettings()
	ConnList = make(map[string]*kafka.Writer, 10)
	acl = &plain.Mechanism{
		Username: "admin",
		Password: "admin-secret",
	}
}

func Produce(topic, header, key, value string) error {
	var writer *kafka.Writer
	var ok bool = false
	if writer, ok = ConnList[topic]; !ok {
		writer = kafka.NewWriter(
			kafka.WriterConfig{
				Brokers: []string{"localhost:9092"},
				Dialer: &kafka.Dialer{
					SASLMechanism: *acl,
				},
				Topic: topic,
			},
		)
		ConnList[topic] = writer
	}
	for i:=0; i<env.PUBLISH_RETRY; i++{
		
	}
	return nil
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
