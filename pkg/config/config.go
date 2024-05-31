package config

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/kelseyhightower/envconfig"
	"runtime"
	"strconv"
	"strings"
)

// Configuration конфиг шарды.
type Configuration struct {
	Namespace      string `envconfig:"NAMESPACE"     required:"true"`
	ApiPort        string `envconfig:"API_PORT"      required:"true"`
	MetricPort     string `envconfig:"METRIC_PORT"   required:"true"`
	ProbePort      string `envconfig:"PROBE_PORT"    required:"true"`
	ProfilerPort   string `envconfig:"PROFILER_PORT" required:"true"`
	DoBackup       bool   `envconfig:"DO_BACKUP"     required:"true"`
	Folder         string `envconfig:"FOLDER"        required:"false"`
	ReplicaCount   uint32 `envconfig:"REPLICA_COUNT" required:"true"`
	Shard          uint32
	UpdateInterval uint   `envconfig:"UPDATE_INTERVAL" required:"true"`
	ServiceName    string `envconfig:"SERVICE_NAME"    required:"true"`

	//redis
	RedisHost         string `envconfig:"REDIS_HOST"     required:"true"`
	RedisPort         string `envconfig:"REDIS_PORT"       required:"true"`
	RedisUser         string `envconfig:"REDIS_USER"       required:"true"`
	RedisPassword     string `envconfig:"REDIS_PASS"       required:"true"`
	RedisReadTimeout  int    `envconfig:"REDIS_READ_TIMEOUT"       required:"true"`
	RedisWriteTimeout int    `envconfig:"REDIS_WRITE_TIMEOUT"       required:"true"`
	RedisMaxPoolSize  int    `envconfig:"REDIS_MAX_POOL_SIZE"       required:"true"`
	//kafka
	//todo посмотреть как кафка лежит в конфигах других сервисов
	KafkaProducer kafka.ConfigMap
	KafkaConsumer kafka.ConfigMap
}

// Read ...
func (c *Configuration) Read() error {
	err := envconfig.Process("", c)
	if err != nil {
		_, filename, line, _ := runtime.Caller(0)
		return fmt.Errorf("\n-> filename-%s:%d, error-read: %w", filename, line, err)
	}

	index, _, err := podIndexFromValue(c.ServiceName)
	if err != nil {
		_, filename, line, _ := runtime.Caller(0)
		return fmt.Errorf("\n-> filename-%s:%d, error-read: %w", filename, line, err)
	}
	c.Shard = uint32(index)
	c.Folder = fmt.Sprintf("%v/", c.Folder)

	return nil
}

// podIndexFromValue.
func podIndexFromValue(val string) (uint, string, error) {
	pos := strings.LastIndex(val, "-")
	if pos < 0 || pos > len(val)-2 {
		return 0, "", fmt.Errorf("string '%s' not conforms required format '<name>-<index>'", val)
	}
	indexString := val[pos+1:]
	index, err := strconv.ParseUint(indexString, 10, 32)
	if err != nil {
		return 0, "", fmt.Errorf("error while parsing index part of pod name: %w", err)
	}

	return uint(index), val[:pos], nil
}
