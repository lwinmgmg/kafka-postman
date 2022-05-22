package environ_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/lwinmgmg/kafka-postman/environ"
)

func TestGetEnv(t *testing.T) {
	env := environ.GetAllEnvSettings()
	fmt.Println(env)
}

func TestReadEnvFile(t *testing.T) {
	envFilePath, ok := os.LookupEnv("KP_ENV_PATH")
	if !ok {
		t.Fatal("Please export KP_ENV_PATH")
	}
	environ.ReadEnvFile(envFilePath)
}

func TestGetDefaultValue(t *testing.T) {
	data := environ.GetDefaultValue("LMM", 10, true)
	if data != "10" {
		t.Errorf("Expecting 10, Getting %v", data)
	}
	data = environ.GetDefaultValue("KP_ENV_PATH", 10, true)
	if data == "10"{
		t.Error("Expecting filepath, Getting 10")
	}
}
