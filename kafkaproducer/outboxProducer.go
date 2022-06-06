package kafkaproducer

import (
	"time"

	"github.com/lwinmgmg/kafka-postman/environ"
	"github.com/lwinmgmg/kafka-postman/kafkaproducer/producerhelper"
	"github.com/lwinmgmg/kafka-postman/logmgr"
	"github.com/lwinmgmg/kafka-postman/models"
)

var (
	chanTopicMap map[string]int
	chanList     []chan uint
	env          *environ.Environ
	done         chan struct{}
	confirm      chan struct{} //confirm channel for all producer worker stopped properly
	callBackChan chan struct{}
	logger       = logmgr.GetLogger()
)

func init() {
	env = environ.GetAllEnvSettings()
	chanList = make([]chan uint, 0, env.PUBLISHER_WORKER)
	confirm = make(chan struct{}, env.PUBLISHER_WORKER)
	done = make(chan struct{})
	callBackChan = make(chan struct{})
	chanTopicMap = make(map[string]int, 20)
	for i := 1; i <= env.PUBLISHER_WORKER; i++ {
		ch := make(chan uint, env.PUBLISHER_QUEUE_COUNT)
		go producerhelper.Service(ch, done, confirm, i)
		chanList = append(chanList, ch)
	}
}

func GetChannelList() []chan uint {
	return chanList
}

func GetDoneChannel() chan struct{} {
	return done
}

func GetCallBackChannel() chan struct{} {
	return callBackChan
}

func ProducerMain() {
	go func() {
		mapIndexCal := 0
		for {
			var outBoxDataList []struct {
				ID    uint
				Topic string
			} = make([]struct {
				ID    uint
				Topic string
			}, 0, env.PUBLISH_LIMIT)
			outboxMgr := models.NewManager(&models.OutBox{})
			if err := outboxMgr.GetByFilter(&outBoxDataList, "state=? ORDER BY id LIMIT ?", models.DRAFT, env.PUBLISH_LIMIT); err != nil {
				logger.Error("%v", err)
			}
			for _, v := range outBoxDataList {
				randomIndex, ok := chanTopicMap[v.Topic]
				if !ok {
					randomIndex = mapIndexCal % env.PUBLISHER_WORKER
					chanTopicMap[v.Topic] = randomIndex
					mapIndexCal += 1
				}
				chanList[randomIndex] <- v.ID
			}
			time.Sleep(time.Millisecond * time.Duration(env.PUBLISH_INTERVAL))
		}
	}()
	confirmSignalCount := 0
	for {
		if confirmSignalCount == env.PUBLISHER_WORKER {
			break
		}
		if _, ok := <-confirm; ok {
			confirmSignalCount += 1
			continue
		}
		break
	}
	for _, v := range chanList {
		close(v)
	}
	callBackChan <- struct{}{}
}
