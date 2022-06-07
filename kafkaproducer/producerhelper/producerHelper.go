package producerhelper

import (
	"github.com/lwinmgmg/kafka-postman/dbm"
	"github.com/lwinmgmg/kafka-postman/kafkamgr"
	"github.com/lwinmgmg/kafka-postman/logmgr"
	"github.com/lwinmgmg/kafka-postman/models"
	"github.com/segmentio/kafka-go/protocol"
	"gorm.io/gorm"
)

var logger = logmgr.GetLogger()

func produceByID(id uint) error {
	outboxMgr := models.NewManager(&models.OutBox{}, dbm.GetDB())
	var datas []models.OutBox
	return outboxMgr.GetForUpdate([]uint{id}, &datas, func(db *gorm.DB) error {
		if len(datas) != 1 || datas[0].State != models.DRAFT {
			return nil
		}
		data := datas[0]
		if err := kafkamgr.Produce(data.Topic, data.Key, data.Value, protocol.Header{Key: "Content-Type", Value: []byte("json")}); err != nil {
			updateData := map[string]any{
				"state":         models.FAILED,
				"failed_reason": err.Error(),
			}
			return outboxMgr.UpdateByIDTx(id, updateData, db)
		}
		updateData := map[string]any{
			"state": models.DONE,
		}
		return outboxMgr.UpdateByIDTx(id, updateData, db)
	})
}

func Service(ch <-chan uint, done <-chan struct{}, confirmChan chan<- struct{}, number int) {
	logger.Info("Publisher Channel [%v] started", number)
	breakSig := false
	for !breakSig {
		select {
		case id, ok := <-ch:
			if !ok {
				breakSig = true
			}
			logger.Info("Channel [%v] got ID [%v]", number, id)
			if err := produceByID(id); err != nil {
				logger.Error("Channel [%v] faild to produce ID [%v]", number, id)
				break
			}
			logger.Info("Channel [%v] done ID [%v]", number, id)
			break
		case <-done:
			breakSig = true
			logger.Info("Publisher Channel [%v] has stopped successfully", number)
		}
	}
	confirmChan <- struct{}{}
}
