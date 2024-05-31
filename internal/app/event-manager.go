package app

import (
	"event-manager/internal/handlers"
	"event-manager/internal/logic"
	"event-manager/internal/repository"
	"event-manager/pkg/config"
	"event-manager/pkg/kafka"
	"event-manager/pkg/logs"
	"event-manager/pkg/redis"
)

func Run(config *config.Configuration) {
	//инициализация глобального логера
	logs.NewLogger()

	//brokers client
	var (
		k = kafka.NewBrokerClient(config.KafkaProducer, config.KafkaConsumer)
	)
	//repository clients
	var (
		redisClient = redis.NewClient(redis.Config{
			Host:         config.RedisHost,
			Port:         config.RedisPort,
			User:         config.RedisUser,
			Password:     config.RedisPassword,
			ReadTimeout:  config.RedisReadTimeout,
			WriteTimeout: config.RedisWriteTimeout,
			MaxPoolSize:  config.RedisMaxPoolSize,
		})
	)

	//repository
	var (
		//redis
		featureRedis = repository.NewFeatureRedisRepository(redisClient)
	)

	//logic
	var (
		eventEnrichmentLogic = logic.NewEventEnrichmentLogic(featureRedis)
	)

	//handlers
	handlers.NewKafkaHandlers(k, eventEnrichmentLogic)

	//service start
	logs.Logger.Info("START KAFKA SERVER")
	err := k.Start()
	if err != nil {
		logs.Logger.Fatal(err, "can't start kafka client")
	}

	select {}
}
