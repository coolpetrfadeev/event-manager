package repository

import (
	"context"
	"event-manager/pkg/redis"
	"fmt"
)

type FeatureRedis interface {
	Get(context.Context) (bool, error)
	Set(context.Context, bool) error
	Delete(context.Context) error
}
type featureRedis struct {
	redisClient redis.RedisClient
}

func NewFeatureRedisRepository(redisClient redis.RedisClient) FeatureRedis {
	return &featureRedis{redisClient: redisClient}
}

/*-------------------------------------------↓ methods realization ↓------------------------------------------------------------------*/

func (f *featureRedis) Get(ctx context.Context) (bool, error) {
	isActive, err := f.redisClient.HGet(
		ctx,
		"someKey",
		"someField").Bool()
	if err != nil {
		return false, fmt.Errorf("err : %w", err)
	}

	return isActive, nil
}

func (f *featureRedis) Set(ctx context.Context, b bool) error {
	resp := f.redisClient.HSet(
		ctx,
		"someKey",
		true)
	if resp.Err() != nil {
		return fmt.Errorf("err : %w", resp.Err())
	}
	return nil
}

func (f *featureRedis) Delete(ctx context.Context) error {
	err := f.redisClient.HDel(
		ctx,
		"someKey",
		"someField")
	if err.Err() != nil {
		return fmt.Errorf("err : %w", err.Err())
	}
	return nil
}
