package kafkaproducer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/lwinmgmg/kafka-postman/environ"
	"github.com/lwinmgmg/kafka-postman/kafkaproducer/producerhelper"
	"github.com/lwinmgmg/kafka-postman/models"
)

var (
	chanList     []chan uint
	env          *environ.Environ
	done         chan struct{}
	confirm      chan struct{} //confirm channel for all producer worker stopped properly
	callBackChan chan struct{}
)

func init() {
	env = environ.GetAllEnvSettings()
	chanList = make([]chan uint, 0, env.PUBLISHER_WORKER)
	confirm = make(chan struct{}, env.PUBLISHER_WORKER)
	done = make(chan struct{})
	callBackChan = make(chan struct{})

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
		for {
			var outBoxIDList []struct {
				ID uint
			} = make([]struct{ ID uint }, 0, env.PUBLISH_LIMIT)
			outboxMgr := models.NewManager(&models.OutBox{})
			if err := outboxMgr.GetByFilter(&outBoxIDList, "state=? ORDER BY id LIMIT ?", models.DRAFT, env.PUBLISH_LIMIT); err != nil {
				fmt.Println(err)
			}
			for _, v := range outBoxIDList {
				randomIndex := rand.Intn(len(chanList))
				chanList[randomIndex] <- v.ID
			}
			time.Sleep(time.Second * time.Duration(env.PUBLISH_INTERVAL))
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
