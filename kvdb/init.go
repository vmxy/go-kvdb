package kvdb

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/redis/go-redis/v9"
)

type RedisOptions struct {
	Host     string
	Port     int
	Password string
	DB       int
}

var mdb *redis.Client
var idb *redis.Client

//var ctx context.Context

func InitRedis(o RedisOptions) {
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
	//	ctx = context.Background()
}

type MemOptions struct {
	dir string
	mem bool
}

var memMdb, memIdb *pebble.DB

func InitMem(o MemOptions) {
	if memMdb != nil {
		return
	}
	if o.mem {
		// 纯内存数据库（数据仅存于内存）
		if db, err := pebble.Open("", &pebble.Options{
			FS: vfs.NewMem(), // 使用内存文件系统
		}); err == nil {
			memMdb = db
		}
		// 纯内存数据库（数据仅存于内存）
		if db, err := pebble.Open("", &pebble.Options{
			FS: vfs.NewMem(), // 使用内存文件系统
		}); err == nil {
			memIdb = db
		}
	} else {
		// 纯内存数据库（数据仅存于内存）
		if db, err := pebble.Open(o.dir, &pebble.Options{
			BytesPerSync: 1 << 20, // 1MB同步一次，提升写入性能
		}); err == nil {
			memMdb = db
		}
		// 纯内存数据库（数据仅存于内存）
		if db, err := pebble.Open(fmt.Sprintf("%s.i", o.dir), &pebble.Options{
			FS: vfs.NewMem(), // 使用内存文件系统
		}); err == nil {
			memIdb = db
		}
	}

}
