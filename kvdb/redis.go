package model

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Options struct {
	Host     string
	Port     int
	Password string
	DB       int
}

var mdb *redis.Client
var idb *redis.Client
var ctx context.Context

func InitRedis(o Options) {
	mdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", o.Host, o.Port), // Redis 服务器地址
		Password: o.Password,                           // 密码
		DB:       o.DB,                                 // 数据库编号
	})
	idb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", o.Host, o.Port), // Redis 服务器地址
		Password: o.Password,                           // 密码
		DB:       o.DB + 1,                             // 数据库编号
	})
	// 测试连接
	ctx = context.Background()
}

func RSet(key string, value any) error {
	return mdb.Set(ctx, key, value, 0).Err()
}
func RGet(key string, value any) (string, error) {
	return mdb.Get(ctx, key).Result()
}
func RDel(keys ...string) (int64, error) {
	return mdb.Del(ctx, keys...).Result()
}
func RExists(keys ...string) (int64, error) {
	return mdb.Exists(ctx, keys...).Result()
}
