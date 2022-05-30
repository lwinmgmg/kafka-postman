package producerhelper

import (
	"fmt"

	"github.com/lwinmgmg/kafka-postman/kafkamgr"
	"github.com/lwinmgmg/kafka-postman/models"
	"github.com/segmentio/kafka-go/protocol"
	"gorm.io/gorm"
)

func produceByID(id uint) error {
	outboxMgr := models.NewManager(&models.OutBox{})
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
	fmt.Printf("Publisher Channel [%v] started\n", number)
	breakSig := false
	for !breakSig {
		select {
		case id, ok := <-ch:
			if !ok {
				breakSig = true
			}
			fmt.Printf("Channel [%v] got ID [%v]\n", number, id)
			if err := produceByID(id); err != nil {
				fmt.Printf("Channel [%v] faild to produce ID [%v]\n", number, id)
				break
			}
			fmt.Printf("Channel [%v] done ID [%v]\n", number, id)
			break
		case <-done:
			breakSig = true
			fmt.Printf("Publisher Channel [%v] has stopped successfully\n", number)
		}
	}
	confirmChan <- struct{}{}
}
