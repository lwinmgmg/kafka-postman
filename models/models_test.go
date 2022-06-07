package models_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lwinmgmg/kafka-postman/dbm"
	"github.com/lwinmgmg/kafka-postman/models"
	"gorm.io/gorm"
)

var (
	idList    []uint          = make([]uint, 0, 3)
	mapNameID map[string]uint = make(map[string]uint, 3)
	testUsers                 = []*DummyData{
		{Name: "lmm", Age: 20, Active: true},
		{Name: "lmm1", Age: 30, Active: true},
		{Name: "lmm2", Age: 40, Active: false},
	}
	topics = []string{
		"topic1",
		"topic2",
		"topic3",
	}
	db *gorm.DB
)

type DummyData struct {
	ID        uint      `gorm:"primarykey" json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Name      string    `json:"name"`
	Age       uint      `json:"age"`
	Active    bool      `json:"active"`
}

func (tb *DummyData) TableName() string {
	return "test_dummydata"
}

func prepareData(db *gorm.DB) {
	for _, v := range testUsers {
		db.Create(v)
		idList = append(idList, v.ID)
		mapNameID[v.Name] = v.ID
	}
}

// func TestPrepareData(t *testing.T) {
// 	mgr := models.NewManager(&models.OutBox{})
// 	for i := 0; i < 100; i++ {
// 		for _, v := range testUsers {
// 			randomIndex := rand.Intn(len(topics))
// 			data, err := json.Marshal(v)
// 			if err != nil {
// 				t.Error(err)
// 			}
// 			mgr.Create(
// 				&models.OutBox{
// 					State:  models.DRAFT,
// 					Topic:  topics[randomIndex],
// 					Header: "erp",
// 					Key:    "id",
// 					Value:  string(data),
// 				},
// 			)
// 		}
// 	}
// }

func TestMain(m *testing.M) {
	db = dbm.GetDB()
	db.AutoMigrate(&DummyData{})
	prepareData(db)
	exitCode := m.Run()
	db.Migrator().DropTable(&DummyData{})
	os.Exit(exitCode)
}

func TestGetByID(t *testing.T) {
	mgr := models.NewManager(&DummyData{}, db)
	var user struct {
		ID   uint
		Name string
	}
	mgr.GetByID(idList[0], &user)
	if user.ID != idList[0] {
		t.Errorf("Expecting user id : %v, Getting : %v", idList[0], user.ID)
	}
}

func TestGetByIDs(t *testing.T) {
	mgr := models.NewManager(&DummyData{}, db)
	var users []struct {
		ID   uint
		Name string
		Age  uint
	}
	if err := mgr.GetByIDs(idList, &users); err != nil {
		t.Errorf("Getting error on getbyids : %v", err)
	}
	for k, v := range testUsers {
		if users[k].ID == 0 {
			t.Errorf("Expecting non Zero value for ID")
		}
		if users[k].Name != v.Name || users[k].Age != v.Age {
			t.Errorf("Expecting name,value : %v,%v, Getting : %v,%v", v.Name, v.Age, users[k].Name, users[k].Age)
		}
	}
}

func TestGetByFilter(t *testing.T) {
	mgr := models.NewManager(&DummyData{}, db)
	var users []struct {
		ID   uint
		Name string
	}
	if err := mgr.GetByFilter(&users, "active=? ORDER BY id LIMIT ?", true, 10); err != nil {
		t.Errorf("Getting error on getbyfilter : %v", err)
	}
	for _, v := range users {
		if v.ID != mapNameID[v.Name] {
			t.Errorf("Expecting id of name %v : %v, Getting : %v", v.Name, v.ID, mapNameID[v.Name])
		}
	}
	var localIdList []struct {
		ID uint
	}
	if err := mgr.GetByFilter(&localIdList, "active=? LIMIT 3", true); err != nil {
		t.Errorf("Getting error on getbyfilter : %v", err)
	}
	fmt.Println(localIdList)
}

func TestUpdateByID(t *testing.T) {
	mgr := models.NewManager(&DummyData{}, db)
	dest := []DummyData{}
	id := idList[0]
	if err := mgr.GetForUpdate([]uint{id}, &dest, func(db *gorm.DB) error {
		updateData := map[string]any{
			"age":  50,
			"name": "lmm50",
		}
		if err := mgr.UpdateByID(id, updateData); err == nil {
			t.Errorf("GetForUpdate Lock is not working")
		}
		updateData["age"] = 100
		if err := mgr.UpdateByIDTx(id, updateData, db); err != nil {
			t.Errorf("Error on updatebyiTx : %v", err)
		}
		return nil
	}); err != nil {
		t.Errorf("Error on getforupdate : %v", err)
	}
	newDest := struct{ Age uint }{}
	if err := mgr.GetByID(id, &newDest); err != nil {
		t.Errorf("Getting error on getbyid : %v", err)
	}
	if newDest.Age != 100 {
		t.Errorf("Expecting age %v, Getting : %v", 100, newDest.Age)
	}
}

func TestGetForUpdate(t *testing.T) {
	mgr := models.NewManager(&DummyData{}, db)
	dest := []DummyData{}
	if err := mgr.GetForUpdate(idList, &dest, func(tx *gorm.DB) error {
		if len(dest) != len(idList) {
			t.Errorf("Expecting length of dest is 2, getting : %v", len(dest))
		}
		for _, v := range dest {
			if err := tx.Model(&DummyData{}).Where("id=?", v.ID).Updates(map[string]interface{}{
				"active": false,
			}).Error; err != nil {
				t.Errorf("Getting error on updating user %v : %v", v.ID, err)
			}
		}
		return nil
	}); err != nil {
		t.Errorf("Getting err : %v", err)
	}
	var user struct {
		ID     uint
		Active bool
	}
	for _, v := range idList {
		if err := mgr.GetByID(v, &user); err != nil {
			t.Errorf("Getting error on getbyid in Getforupdate : %v", err)
		}
		if user.Active != false {
			t.Errorf("Expacting false after updating, getting : %v", user.Active)
		}
	}
}
