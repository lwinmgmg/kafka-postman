package producers

import (
	"github.com/lwinmgmg/kafka-postman/kafkamgr"
	"github.com/lwinmgmg/kafka-postman/models"
	"github.com/segmentio/kafka-go/protocol"
	"gorm.io/gorm"
)

type OutBoxManager struct {
	// Read data from readonly or slave instance
	ReaderModelMgr *models.Manager
	// Write data to master instance
	WriterModelMgr *models.Manager
}

func (mgr *OutBoxManager) ProduceByID(id uint, kafkaServer *kafkamgr.KafkaServer) error {
	var datas []models.OutBox
	return mgr.WriterModelMgr.GetForUpdate([]uint{id}, &datas, func(db *gorm.DB) error {
		if len(datas) != 1 || datas[0].State != models.DRAFT {
			return nil
		}
		data := datas[0]
		if err := kafkaServer.Produce(data.Topic, data.Key, data.Value, protocol.Header{Key: "Content-Type", Value: []byte("json")}); err != nil {
			updateData := map[string]any{
				"state":         models.FAILED,
				"failed_reason": err.Error(),
			}
			return mgr.WriterModelMgr.UpdateByIDTx(id, updateData, db)
		}
		updateData := map[string]any{
			"state": models.DONE,
		}
		return mgr.WriterModelMgr.UpdateByIDTx(id, updateData, db)
	})
}

func (mgr OutBoxManager) GetDraftProducerData(limit int) ([]ProducerData, error) {
	var outBoxDataList []ProducerData
	if err := mgr.ReaderModelMgr.GetByFilter(&outBoxDataList, "state=? ORDER BY id LIMIT ?", models.DRAFT, limit); err != nil {
		return nil, err
	}
	return outBoxDataList, nil
}

// func init() {
// 	env = environ.GetAllEnvSettings()
// 	chanList = make([]chan uint, 0, env.PUBLISHER_WORKER)
// 	confirm = make(chan struct{}, env.PUBLISHER_WORKER)
// 	done = make(chan struct{})
// 	callBackChan = make(chan struct{})
// 	chanTopicMap = make(map[string]int, 20)
// 	for i := 1; i <= env.PUBLISHER_WORKER; i++ {
// 		ch := make(chan uint, env.PUBLISHER_QUEUE_COUNT)
// 		// go producerhelper.Service(ch, done, confirm, i)
// 		chanList = append(chanList, ch)
// 	}
// }

// func GetChannelList() []chan uint {
// 	return chanList
// }

// func GetDoneChannel() chan struct{} {
// 	return done
// }

// func GetCallBackChannel() chan struct{} {
// 	return callBackChan
// }

// func ProducerMain() {
// 	go func() {
// 		mapIndexCal := 0
// 		for {
// 			var outBoxDataList []struct {
// 				ID    uint
// 				Topic string
// 			} = make([]struct {
// 				ID    uint
// 				Topic string
// 			}, 0, env.PUBLISH_LIMIT)
// 			outboxMgr := models.NewManager(&models.OutBox{}, dbm.GetDB())
// 			if err := outboxMgr.GetByFilter(&outBoxDataList, "state=? ORDER BY id LIMIT ?", models.DRAFT, env.PUBLISH_LIMIT); err != nil {
// 				logger.Error("%v", err)
// 			}
// 			for _, v := range outBoxDataList {
// 				randomIndex, ok := chanTopicMap[v.Topic]
// 				if !ok {
// 					randomIndex = mapIndexCal % env.PUBLISHER_WORKER
// 					chanTopicMap[v.Topic] = randomIndex
// 					mapIndexCal += 1
// 				}
// 				chanList[randomIndex] <- v.ID
// 			}
// 			time.Sleep(time.Millisecond * time.Duration(env.PUBLISH_INTERVAL))
// 		}
// 	}()
// 	confirmSignalCount := 0
// 	for {
// 		if confirmSignalCount == env.PUBLISHER_WORKER {
// 			break
// 		}
// 		if _, ok := <-confirm; ok {
// 			confirmSignalCount += 1
// 			continue
// 		}
// 		break
// 	}
// 	for _, v := range chanList {
// 		close(v)
// 	}
// 	callBackChan <- struct{}{}
// }
