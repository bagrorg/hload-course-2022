package main

import (
	"context"

	"github.com/go-redis/redis/v8"
)

var redisOpts redis.Options = redis.Options{
	Addr:     "***",
	Password: "", // no password set
	DB:       0,  // use default DB
}

func buildKey(tinyUrl string) string {
	return "dkulikov:urls:" + tinyUrl
}

func GetLongUrl(tinyUrl string) (string, error) {
	ctx := context.Background()
	r := redis.NewClient(&redisOpts)
	return r.Get(ctx, buildKey(tinyUrl)).Result()
}

func SetLongUrl(tinyUrl string, longUrl string) error {
	ctx := context.Background()
	r := redis.NewClient(&redisOpts)

	_, err := r.Set(ctx, tinyUrl, longUrl, 0).Result()
	return err
}
