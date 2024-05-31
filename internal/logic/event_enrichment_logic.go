package logic

import (
	"context"
	"event-manager/internal/repository"
	"fmt"
)

type eventEnrichmentLogic struct {
	featureRedisRepo repository.FeatureRedis
}

func NewEventEnrichmentLogic(featureRedisRepo repository.FeatureRedis) EventEnrichmentLogic {
	return &eventEnrichmentLogic{
		featureRedisRepo: featureRedisRepo,
	}
}

type EventEnrichmentLogic interface {
	SomeWork(ctx context.Context, data []byte) error
}

/*-------------------------------------------↓ methods realization ↓------------------------------------------------------------------*/

func (e *eventEnrichmentLogic) SomeWork(ctx context.Context, data []byte) error {
	fmt.Println(data)
	flag, err := e.featureRedisRepo.Get(ctx)
	if err != nil {
		return fmt.Errorf("cant get : %w", err.Error())
	}
	fmt.Println(flag)
	err = e.featureRedisRepo.Delete(ctx)
	if err != nil {
		return fmt.Errorf("cant delete : %w", err.Error())
	}
	return nil
}
