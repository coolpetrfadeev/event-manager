package redis

import (
	"context"
	"event-manager/pkg/logs"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"time"
)

const (
	connectionCheckInterval = time.Minute * 10
	reconnectInterval       = time.Second * 5
)

type RedisClient interface {
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HDel(ctx context.Context, key, field string) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
}

type redisClient struct {
	config           Config
	client           *redis.Client
	connectionStatus ConnectionStatus
}

func NewClient(config Config) RedisClient {
	client := redisClient{
		config: config,
	}
	ok := client.setConnection()
	if !ok {
		logs.Logger.Warn("failed to connect to redis. Exiting...")
		os.Exit(1)
	}
	go client.supportConnection()
	return &client
}

func (r *redisClient) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return r.client.HGet(ctx, key, field)
}

func (r *redisClient) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return r.client.HSet(ctx, key, values)
}

func (r *redisClient) HIncr(ctx context.Context, key string, field string) *redis.IntCmd {
	return r.client.HIncrBy(ctx, key, field, 1)
}

func (r *redisClient) HDecr(ctx context.Context, key string, field string) *redis.IntCmd {
	return r.client.HIncrBy(ctx, key, field, -1)
}

func (r *redisClient) HDel(ctx context.Context, key, field string) *redis.IntCmd {
	return r.client.HDel(ctx, key, field)
}

func (r *redisClient) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	return r.client.HGetAll(ctx, key)
}

func (r *redisClient) setConnection() (ok bool) {
	r.client = redis.NewClient(&redis.Options{
		Addr:                  fmt.Sprintf("%v:%v", r.config.Host, r.config.Port),
		Username:              r.config.User,
		Password:              r.config.Password,
		ClientName:            r.serviceName,
		ReadTimeout:           time.Duration(r.config.ReadTimeout) * time.Second,
		WriteTimeout:          time.Duration(r.config.WriteTimeout) * time.Second,
		PoolFIFO:              true,
		PoolSize:              r.config.MaxPoolSize,
		ContextTimeoutEnabled: true,
	})
	if !r.checkConnection() {
		logs.Logger.Warn("Redis client not ready")
		return false
	}
	return true
}

func (r *redisClient) checkConnection() (ok bool) {
	if r.client == nil {
		return false
	}
	err := r.client.Ping(context.Background()).Err()
	if err != nil {
		logs.Logger.Error("Cant ping redis client,", err)
		r.connectionStatus.SetClosedStatus()
		return false
	}
	r.connectionStatus.SetReadyStatus()
	return true
}

func (r *redisClient) supportConnection() {
	for {
		time.Sleep(connectionCheckInterval)
		if !r.checkConnection() {
			r.reconnect()
		}
	}
}

func (r *redisClient) reconnect() {
	if r.client != nil {
		err := r.client.Close()
		if err != nil {
			logs.Logger.Errorf("Redis client close with error,", err.Error())
		}
	}
	logs.Logger.Warn("start reconnect to redis")
	for {
		ok := r.setConnection()
		if !ok {
			time.Sleep(reconnectInterval)
			continue
		}
		return
	}
}
