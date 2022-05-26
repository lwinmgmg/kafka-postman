package environ_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/lwinmgmg/kafka-postman/environ"
)

var tmpFilePath string = fmt.Sprintf("%v/test.env", os.TempDir())

func TestMain(m *testing.M) {
	beforeEnv, ok := os.LookupEnv("KP_ENV_PATH")
	if err := os.Setenv("KP_ENV_PATH", tmpFilePath); err != nil {
		os.Exit(1)
	}
	fmt.Println("Before")
	exitCode := m.Run()
	fmt.Println("Done")
	if ok {
		if err := os.Setenv("KP_ENV_PATH", beforeEnv); err != nil {
			os.Exit(1)
		}
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func TestGetEnv(t *testing.T) {
	env := environ.GetAllEnvSettings()
	fmt.Println(env)
}

func TestReadEnvFile(t *testing.T) {
	file, err := os.Create(tmpFilePath)
	if err != nil {
		t.Error("Error on create test.env tmp file")
	}
	defer file.Close()
	file.Write([]byte("PUBLISHR_WORKER = 10"))
	file.Write([]byte("CONSUMER_WORKER = 20"))
	file.Write([]byte("SETTING_JSON_PATH = abc.json"))
	environ.ReadEnvFile(tmpFilePath)
	v, err := strconv.Atoi(os.Getenv("PUBLISHR_WORKER"))
	if err != nil{
		t.Error("Getting error on reading env :", err)
	}
	if v != 10{
		t.Errorf("Expected %v, Got %v", 10, v)
	}
	v, err = strconv.Atoi(os.Getenv("CONSUMER_WORKER"))
	if err != nil{
		t.Error("Getting error on reading env :", err)
	}
	if v != 20{
		t.Errorf("Expected %v, Got %v", 20, v)
	}
}

func TestGetDefaultValue(t *testing.T) {
	data := environ.GetDefaultValue("LMM", 10, true)
	if data != "10" {
		t.Errorf("Expecting 10, Getting %v", data)
	}
	data = environ.GetDefaultValue("KP_ENV_PATH", 10, true)
	if data == "10" {
		t.Error("Expecting filepath, Getting 10")
	}
}
