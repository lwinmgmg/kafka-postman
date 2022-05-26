package dbm

import (
	"github.com/lwinmgmg/kafka-postman/dbm/psql"
	"github.com/lwinmgmg/kafka-postman/environ"
	"gorm.io/gorm"
)

var db *gorm.DB
var env *environ.Environ = environ.GetAllEnvSettings()

func init() {
	if env.DB_TYPE == "PSQL" {
		db = psql.GetPsql1()
	}
}

func GetDB() *gorm.DB {
	return db
}
