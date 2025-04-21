package kvdb

import (
	"errors"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/exp/rand"
)

/*
// 自定义 MarshalJSON 方法
func marshalJson(v any) ([]byte, error) {
	return sonic.Marshal(v)
}

// 自定义 UnmarshalJSON 方法
func unmarshalJson(data []byte, val any) error {
	return sonic.Unmarshal(data, val)
}
*/
// 泛型函数：将 any 类型转换为泛型类型 T
func unmarshal[T any](val []byte) (obj T, err error) {
	if len(val) < 2 {
		return obj, errors.New("is no json")
	}
	err = msgpack.Unmarshal(val, &obj)
	return obj, err
}
func marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func is[T any](ok bool, yes T, no T) T {
	if ok {
		return yes
	} else {
		return no
	}
}

const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomString(length int) string {
	rand.Seed(uint64(time.Now().UnixNano())) // 初始化随机种子
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
