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
	PUBLISHR_WORKER   int
	CONSUMER_WORKER   int
	SETTING_JSON_PATH string
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
	publisherWorker := toInt(GetDefaultValue("PUBLISHR_WORKER", 2, true))
	env = &Environ{
		PUBLISHR_WORKER: publisherWorker,
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
