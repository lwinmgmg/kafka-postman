package psql_test

import (
	"os"
	"testing"

	"github.com/lwinmgmg/kafka-postman/dbm/psql"
	"github.com/lwinmgmg/kafka-postman/environ"
	"gorm.io/gorm"
)

var db *gorm.DB
var env *environ.Environ = environ.GetAllEnvSettings()

type TableTest struct {
	gorm.Model
	name string
}

func (tb *TableTest) TableName() string {
	return "test_table"
}

func TestMain(m *testing.M) {
	if env.DB_TYPE == "PSQL" {
		db = psql.GetPsql1()
		db.AutoMigrate(&TableTest{})
		exitCode := m.Run()
		db.Migrator().DropTable(&TableTest{})
		os.Exit(exitCode)
	}
}

func TestGetPsql1(t *testing.T) {
	testTable := TableTest{
		name: "LMM",
	}
	err := db.Create(&testTable).Error
	if err != nil {
		t.Errorf("Error getting on db connection : %v", err)
	}
	if testTable.ID == 0 {
		t.Errorf("Table is not created")
	}
}
