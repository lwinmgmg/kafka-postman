package producers

import (
	"time"

	"github.com/lwinmgmg/kafka-postman/environ"
	"github.com/lwinmgmg/kafka-postman/kafkamgr"
	"github.com/lwinmgmg/kafka-postman/logmgr"
)

var (
	logger = logmgr.GetLogger()
	env    = environ.GetAllEnvSettings()
)

type ProducerData struct {
	ID    uint
	Topic string
}

type ModelProducer interface {
	ProduceByID(id uint, kafkaServer *kafkamgr.KafkaServer) error
	GetDraftProducerData(int) ([]ProducerData, error)
}

type Producer struct {
	TopicChannelMap map[string]int
	ProcessList     []chan uint
	QueueCount      int
	KafkaServer     *kafkamgr.KafkaServer
	ModelProducer   ModelProducer
	confirmCh       chan struct{}
	done            chan struct{}
}

func NewProducer(workers int, queuePerWorker int, kafkaServer *kafkamgr.KafkaServer, modelProducer ModelProducer) *Producer {
	producer := &Producer{
		TopicChannelMap: make(map[string]int, workers),
		ProcessList:     make([]chan uint, workers),
		QueueCount:      queuePerWorker,
		KafkaServer:     kafkaServer,
		ModelProducer:   modelProducer,
		confirmCh:       make(chan struct{}, workers+1),
		done:            make(chan struct{}),
	}
	return producer
}

func (producer *Producer) Start() {
	for i := 0; i < len(producer.ProcessList); i++ {
		producer.ProcessList[i] = make(chan uint, producer.QueueCount)
		go producer.Service(i)
	}
	stopSig := false
	go func() {
		for !stopSig {
			mapIndexCal := 0
			producerDatas, err := producer.ModelProducer.GetDraftProducerData(env.PUBLISH_LIMIT)
			if err != nil {
				logger.Error("Error on getting producer data : %v", err)
			}
			for _, v := range producerDatas {
				randomIndex, ok := producer.TopicChannelMap[v.Topic]
				if !ok {
					randomIndex = mapIndexCal % len(producer.ProcessList)
					producer.TopicChannelMap[v.Topic] = randomIndex
					mapIndexCal += 1
				}
				producer.ProcessList[randomIndex] <- v.ID
			}
			time.Sleep(time.Millisecond * time.Duration(env.PUBLISH_INTERVAL))
		}
		producer.confirmCh <- struct{}{}
	}()
	go func(ch <-chan struct{}) {
		<-ch
		stopSig = true
	}(producer.done)
}

func (producer *Producer) Stop() {
	close(producer.done)
	for i := 0; i <= len(producer.ProcessList); i++ {
		<-producer.confirmCh
	}
}

func (producer *Producer) Service(processIndex int) {
	logger.Info("Publisher Channel [%v] started", processIndex+1)
	number := processIndex + 1
	breakSig := false
	for !breakSig {
		select {
		case id, ok := <-producer.ProcessList[processIndex]:
			if !ok {
				breakSig = true
			}
			logger.Info("Channel [%v] got ID [%v]", number, id)
			if err := producer.produceByID(id); err != nil {
				logger.Error("Channel [%v] faild to produce ID [%v]", number, id)
				break
			}
			logger.Info("Channel [%v] done ID [%v]", number, id)
			break
		case <-producer.done:
			breakSig = true
			logger.Info("Publisher Channel [%v] has stopped successfully", number)
		}
	}
	producer.confirmCh <- struct{}{}
}

func (producer *Producer) produceByID(id uint) error {
	return producer.ModelProducer.ProduceByID(id, producer.KafkaServer)
}
