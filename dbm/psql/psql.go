package psql

import (
	"fmt"

	"github.com/lwinmgmg/kafka-postman/environ"
	"gorm.io/driver/postgres"
	_ "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var psql1 *gorm.DB
var env *environ.Environ = environ.GetAllEnvSettings()

func init() {
	dsn1 := "user=opi password=letmein dbname=outbox port=5432 sslmode=disable TimeZone=Asia/Yangon"
	var err error
	if env.DB_TYPE == "PSQL"{
		if psql1, err = ConnectPG(dsn1); err != nil {
			panic(fmt.Sprintf("Error on connecting postgres %v : %v", dsn1, err))
		}
	}
}

func ConnectPG(dsn string) (*gorm.DB, error) {
	return gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true,
	}), &gorm.Config{})
}

func GetPsql1() *gorm.DB {
	return psql1
}
