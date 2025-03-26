package kvdb

import (
	"errors"

	"github.com/vmihailenco/msgpack/v5"
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
