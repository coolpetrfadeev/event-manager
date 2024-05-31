package kafka

import (
	"context"
	"encoding/json"
	"event-manager/pkg/logs"
	"fmt"
	"github.com/CossackPyra/pyraconv"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/labstack/gommon/log"
	"sync"
	"time"
)

const (
	// Максимальное количество реплик каждой партиции (равно количеству брокеров в кластере)
	maxReplicationFactor = 3

	// Значение реплик каждой партиции по умолчанию
	defaultReplicationFactor = 1
	// Значение партиций для топика по умолчанию
	defaultNumPartitions = 3

	// Время ожидания, пока очередь в буфере продусера переполнена
	queueFullWaitTime = time.Second * 5

	reconnectTime = time.Second * 10

	flushTimeout = 5000

	readTimeout = time.Second
)

type Handler func(ctx context.Context, message []byte) error

type BrokerClient interface {
	Start() error
	Pre(mw ...interface{})
	StopSubscribe()
	StopProduce()
	Publish(context.Context, string, interface{}) error
	Subscribe(string, Handler, int, *TopicSpecifications, ...bool)
	PublishByte(ctx context.Context, topic string, message []byte) (err error)
}

type TopicSpecifications struct {
	NumPartitions     int
	ReplicationFactor int
}

type (
	messageHandler func(ctx context.Context, message *kafka.Message) error
	//midleware если понадобится
	//MiddlewareFunc func(next messageHandler) messageHandler
)

type kafkaConsumer struct {
	topic      string
	checkError bool
	handler    Handler
	*kafka.Consumer
}

type subscription struct {
	topic      string
	checkError bool
	goroutines int
	spec       *TopicSpecifications
	handler    Handler
}

type client struct {
	producer    *kafka.Producer
	consumers   []kafkaConsumer
	subscribers []subscription
	//mwFuncs        []MiddlewareFunc
	producerConfig kafka.ConfigMap
	consumerConfig kafka.ConfigMap

	gCtx   context.Context
	cancel context.CancelFunc
}

func (c *client) Start() (err error) {
	err = c.initProducer(c.producerConfig)
	if err != nil {
		return
	}
	if len(c.subscribers) != 0 {
		err = c.createTopics()
		if err != nil {
			return
		}
	}
	err = c.initConsumers(c.consumerConfig)
	if err != nil {
		return
	}
	return
}

func (c *client) Pre(mw ...interface{}) {
	//mw если надо будет
	/*for _, v := range mw {
		c.mwFuncs = append(c.mwFuncs, v.(MiddlewareFunc))
	}*/
}

func (c *client) StopSubscribe() {

	c.cancel()

	for i := range c.consumers {
		_, err := c.consumers[i].Commit()
		if err != nil {
			logs.Logger.Errorf("cant commit offset for topic: %s", err.Error())
		}
		// Отписка от назначенных топиков
		err = c.consumers[i].Unsubscribe()
		if err != nil {
			logs.Logger.Errorf("cant unsubscribe connection: %s", err.Error())
		}
		// Закрытие соединения
		err = c.consumers[i].Close()
		if err != nil {
			logs.Logger.Errorf("cant close consumer connection: %s", err.Error())
		}
	}
}

func (c *client) StopProduce() {
	c.producer.Flush(flushTimeout)
	c.producer.Close()
}

func (c *client) Publish(ctx context.Context, topic string, data interface{}) (err error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("cant marshall data for kafka:%w", err)
	}
	return c.PublishByte(ctx, topic, dataBytes)
}

func (c *client) Subscribe(topic string, h Handler, goroutines int, spec *TopicSpecifications, checkError ...bool) {
	s := subscription{
		topic:      topic,
		handler:    h,
		spec:       spec,
		goroutines: goroutines,
	}
	if len(checkError) != 0 {
		s.checkError = checkError[0]
	}
	c.subscribers = append(c.subscribers, s)
}

func (c *client) PublishByte(ctx context.Context, topic string, message []byte) (err error) {

	deliveryChannel := make(chan kafka.Event)

	go c.handleDelivery(ctx, topic, message, deliveryChannel)

	err = c.produce(
		ctx,
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          message,
		},
		deliveryChannel,
	)
	if err != nil {
		return err
	}
	return
}

func (c *client) handleDelivery(ctx context.Context, topic string, message []byte, deliveryChannel chan kafka.Event) {

	e := <-deliveryChannel
	close(deliveryChannel)
	switch event := e.(type) {
	case *kafka.Message:
		if event.TopicPartition.Error != nil {
			kafkaErr := event.TopicPartition.Error.(kafka.Error)
			// Если retriable, то ошибка временная, нужно пытаться переотправить снова, если нет, то ошибка nonretriable, просто логируем
			if kafkaErr.IsRetriable() {
				logs.Logger.Errorf("kafka produce retriable error, try again send topic: %v, message: %v", topic, string(message))

				err := c.PublishByte(ctx, topic, message)
				if err != nil {
					logs.Logger.Errorf("Cant publish by kafka, topic: %v, message: %v", topic, string(message))
				}
			} else {
				logs.Logger.Errorf("kafka produce nonretriable error, can't send topic: %v, message: %v. Is fatal: %v",
					topic, string(message), kafkaErr.IsFatal())
			}
		}
	case kafka.Error:
		// Общие пользовательские ошибки, клиент сам пытается переотправить, просто логируем
		logs.Logger.Errorf("publish error, topic: %v, message: %v. client tries to send again", topic, string(message))
	}
}

func (c *client) produce(ctx context.Context, message *kafka.Message, deliveryChannel chan kafka.Event) error {
	for {
		err := c.producer.Produce(message, deliveryChannel)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Если очередь забита, пробуем отправить снова через 5 секунд
				logs.Logger.Warnf("kafka queue full, try again after %v second", queueFullWaitTime.Seconds())
				time.Sleep(queueFullWaitTime)
				continue
			} else {
				return err
			}
		}
		break
	}
	return nil
}

func (c *client) createTopics() (err error) {
	// Создаём админский клиент через настройки подключения продусера
	adminClient, err := kafka.NewAdminClientFromProducer(c.producer)
	if err != nil {
		return fmt.Errorf("cant init kafka admin client:%w", err)
	}
	defer adminClient.Close()
	specifications := make([]kafka.TopicSpecification, 0, len(c.subscribers))
	for _, subscriber := range c.subscribers {
		specification := kafka.TopicSpecification{
			Topic:             subscriber.topic,
			ReplicationFactor: defaultReplicationFactor,
			NumPartitions:     defaultNumPartitions,
		}
		// Если нет настроек топика, то при создании будут подставляться дефолтные
		if subscriber.spec != nil {
			// проверка, чтобы фактор репликации не был больше числа брокеров
			if subscriber.spec.ReplicationFactor > maxReplicationFactor {
				subscriber.spec.ReplicationFactor = maxReplicationFactor
				log.Warnf("Number of replicas cannot be more than %v, set the maximum value", maxReplicationFactor)
			}
			specification.NumPartitions = subscriber.spec.NumPartitions
			specification.ReplicationFactor = subscriber.spec.ReplicationFactor
		}
		specifications = append(specifications, specification)
	}
	result, err := adminClient.CreateTopics(context.Background(), specifications)
	if err != nil {
		return fmt.Errorf("cant create topics:%w", err.Error())
	}
	for _, v := range result {
		// Если такой топик уже есть, то будет ошибка внутри структуры, если ошибки нет, то в структуре будет "Success"
		log.Infof("%v: %v", v.Topic, v.Error.String())
	}
	return nil
}

func (c *client) initProducer(config kafka.ConfigMap) (err error) {

	//на свякий случай удостоверяемся что передали инты конфигу
	config["queue.buffering.max.messages"] = int(pyraconv.ToInt64(config["queue.buffering.max.messages"]))
	config["linger.ms"] = int(pyraconv.ToInt64(config["linger.ms"]))

	c.producer, err = kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("cant create kafka producer:%w", err.Error())
	}

	logs.Logger.Info("KAFKA PRODUCER IS READY")
	return nil
}

func (c *client) initConsumers(config kafka.ConfigMap) error {
	c.gCtx, c.cancel = context.WithCancel(context.Background())

	for _, subscriber := range c.subscribers {
		for i := 0; i < subscriber.goroutines; i++ {
			// Создаём консумера
			consumer, err := kafka.NewConsumer(&config)

			if err != nil {
				return fmt.Errorf("cant create kafka consumer: %w", err.Error())
			}
			// Подписываем консумера на топик
			err = consumer.Subscribe(subscriber.topic, nil)
			if err != nil {
				return fmt.Errorf("cant subscribe kafka consumer: %w", err.Error())
			}
			c.consumers = append(c.consumers, kafkaConsumer{
				topic:      subscriber.topic,
				checkError: subscriber.checkError,
				handler:    subscriber.handler,
				Consumer:   consumer,
			})
		}
	}

	// Запускаем каждого консумера в отдельной горутине
	once := &sync.Once{}
	for _, consumer := range c.consumers {
		go c.startConsume(once, consumer)
	}
	logs.Logger.Info("KAFKA CONSUMERS IS READY")
	return nil
}

func (c *client) startConsume(once *sync.Once, consumer kafkaConsumer) {

	// Прогоняем хендлер через миддлверы
	var handler messageHandler = func(ctx context.Context, message *kafka.Message) error {
		return consumer.handler(ctx, message.Value)
	}
	/*
		for j := len(c.mwFuncs) - 1; j >= 0; j-- {
			handler = c.mwFuncs[j](handler)
		}*/
	for {
		select {
		case <-c.gCtx.Done():
			return
		default:
			msg, err := consumer.ReadMessage(readTimeout)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok {
					// Если retriable (но со стороны консумера вроде бы такого нет), то пробуем снова
					if kafkaErr.IsRetriable() || kafkaErr.Code() == kafka.ErrTimedOut {
						continue
					}
				}
				logs.Logger.Error("cant read kafka message")
				go once.Do(c.reconnect)
				return
			}
			err = handler(context.Background(), msg)
			if err != nil && consumer.checkError {
				logs.Logger.Errorf("try to read message again: %v", err)
				rollbackConsumerTransaction(consumer, msg.TopicPartition)
			}
		}
	}
}

func rollbackConsumerTransaction(consumer kafkaConsumer, topicPartition kafka.TopicPartition) {
	// В committed лежит массив из одного элемента, потому что передаём одну партицию, которую нужно сбросить
	committed, err := consumer.Committed([]kafka.TopicPartition{{Topic: &consumer.topic, Partition: topicPartition.Partition}}, -1)
	if err != nil {
		logs.Logger.Error(err.Error())
		return
	}
	if committed[0].Offset < 0 {
		committed[0].Offset = kafka.OffsetBeginning
	} else {
		committed[0].Offset = topicPartition.Offset
	}
	err = consumer.Seek(committed[0], 0)
	if err != nil {
		logs.Logger.Error(err.Error())
		return
	}
	return
}

func (c *client) reconnect() {

	log.Debug("start reconnecting consumers")
	// Стопаем консумеры
	c.StopSubscribe()
	log.Debugf("consumers stopped")
	// Чистим массив остановленных консумеров
	c.consumers = make([]kafkaConsumer, 0)

	// Ждём 10 секунд для реконнекта
	time.Sleep(reconnectTime)

	// Запускаем новые консумеры
	for {
		err := c.initConsumers(c.consumerConfig)
		if err != nil {
			logs.Logger.Error("cant init consumers")
			time.Sleep(reconnectTime)
			continue
		}
		log.Debugf("new consumers initiated")
		break
	}
}

func NewBrokerClient(producer kafka.ConfigMap, consumer kafka.ConfigMap) BrokerClient {
	return &client{
		producerConfig: producer,
		consumerConfig: consumer,
	}

}

func Start(client BrokerClient) {

	logs.Logger.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		logs.Logger.Fatal(err, "can't start kafka client")
	}
}
