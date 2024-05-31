package handlers

import (
	"context"
	"event-manager/internal/logic"
	"event-manager/pkg/kafka"
	"fmt"
)

const (
	topicName = "some_topic_name"
)

type kafkaHandlers struct {
	eventEnrichmentLogic logic.EventEnrichmentLogic
}

func NewKafkaHandlers(brokerClient kafka.BrokerClient, eventEnrichmentLogic logic.EventEnrichmentLogic) {
	k := kafkaHandlers{eventEnrichmentLogic: eventEnrichmentLogic}

	//some topic description
	brokerClient.Subscribe(topicName, k.collectEvents, 1, &kafka.TopicSpecifications{
		NumPartitions:     3,
		ReplicationFactor: 1,
	})

}

/*-------------------------------------------↓ methods realization ↓------------------------------------------------------------------*/

func (k *kafkaHandlers) collectEvents(ctx context.Context, data []byte) error {
	/*var someStruct struct {
		PersonId int64 `json:"personId"`
		CardId   int64 `json:"cardId"`
	}
	err := json.Unmarshal(data, &someStruct)
	if err != nil {
		return fmt.Errorf("cant unmarshal: %w", err.Error())
	}
	*/

	err := k.eventEnrichmentLogic.SomeWork(ctx, data)
	if err != nil {
		return fmt.Errorf("cant perform logic : %w", err.Error())
	}
	return nil
}
