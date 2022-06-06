package environ

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var env *Environ

type Environ struct {
	PUBLISHER_WORKER      int
	PUBLISHER_QUEUE_COUNT int
	PUBLISH_LIMIT         int
	PUBLISH_INTERVAL      int
	PUBLISH_RETRY         int
	CONSUMER_WORKER       int
	SETTING_JSON_PATH     string
	DB_TYPE               string
	DB_DSN                string
	TABLE_PREFIX          string
}

func GetAllEnvSettings() *Environ {
	return env
}

func init() {
	envFilePath, ok := os.LookupEnv("KP_ENV_PATH")
	if !ok {
		panic("Please export KP_ENV_PATH")
	}
	ReadEnvFile(envFilePath)
	publisherWorker := toInt(GetDefaultValue("PUBLISHER_WORKER", 5, true))
	env = &Environ{
		PUBLISHER_WORKER:      publisherWorker,
		PUBLISHER_QUEUE_COUNT: 10, //buffer size for channel
		PUBLISH_LIMIT:         20,
		PUBLISH_INTERVAL:      2000,
		PUBLISH_RETRY:         5,
		DB_TYPE:               "PSQL",
		TABLE_PREFIX:          "kp",
	}
}

func toInt(input string) int {
	input = strings.Trim(input, " ")
	output, err := strconv.Atoi(input)
	if err != nil {
		panic(fmt.Sprintf("Value [%v] can't convert to integer", input))
	}
	return output
}

func GetDefaultValue(key string, value any, hasDefalut bool) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	if hasDefalut {
		return fmt.Sprint(value)
	}
	panic(fmt.Sprintf("Key [%v] value not found!", key))
}

func ReadEnvFile(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("Can't read %v file : %v", filePath, err))
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		index := strings.Index(line, "=")
		if index < 0 {
			continue
		}
		key := line[:index]
		if _, ok := os.LookupEnv(key); !ok {
			os.Setenv(key, line[index+1:])
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("Error on getting env : %v", err))
	}
}
