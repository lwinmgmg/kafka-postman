package producerhelper

import (
	"fmt"

	"github.com/lwinmgmg/kafka-postman/models"
	"gorm.io/gorm"
)

func produceByID(id uint) error {
	outboxMgr := models.NewManager(&models.OutBox{})
	var datas []models.OutBox
	outboxMgr.GetForUpdate([]uint{id}, &datas, func(d *gorm.DB) error {
		return nil
	})
	return nil
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
			continue
		case <-done:
			breakSig = true
			fmt.Printf("Publisher Channel [%v] has stopped successfully\n", number)
		}
	}
	confirmChan <- struct{}{}
}
