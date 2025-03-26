package kvdb

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime/debug"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

type IndexInfo struct {
	Name  string
	Field string
	Type  string
}
type Table[T any] interface {
	Name() string
	//Name   string
	//mdb    *redis.Client
	//idb    *redis.Client
	//Indexs() map[string]IndexInfo
	Get(id string) (value T, ok bool)
	Gets(ids ...string) (list []T)
	Insert(id string, t T) error
	Update(id string, t T) error
	Delete(id ...string)
	Search(id string, filter func(t T) bool, start_end ...int) []T
	Scan(handle func(t T) bool)
}

// 泛型函数：将 any 类型转换为泛型类型 T
func to[T any](val []byte) (obj T, err error) {
	if len(val) < 2 {
		return obj, errors.New("is no json")
	}
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				log.Printf("toobject error %s\r\n ", string(val))
				debug.PrintStack()
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	err = msgpack.Unmarshal(val, &obj)
	return obj, err
}

func concat[T any](oldVal T, newVal T) T {
	rstruct := getRefTypeElem(oldVal)
	rvalue := getRefValueElem(newVal)
	for i := range rstruct.NumField() {
		field := rstruct.Field(i)
		valueNew := rvalue.FieldByName(field.Name)
		oldValue := getValue(oldVal, field.Name)
		if valueNew.IsZero() && valueNew.CanSet() {
			valueNew.Set(oldValue)
			//value.Set(getValue(oldVal, field.Name))
		}
	}
	return newVal
}

func getValue(entity any, field string) reflect.Value {
	rvalue := getRefValueElem(entity)
	if rvalue.IsValid() {
		return rvalue.FieldByName(field)
	} else {
		return reflect.Value{}
	}
}
func getRefValueElem(entity any) reflect.Value {
	v := reflect.ValueOf(entity)
	for {
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		} else {
			break
		}
	}
	return v
}
func getRefTypeElem(entity any) reflect.Type {
	v := reflect.TypeOf(entity)
	for {
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		} else {
			break
		}
	}
	return v
}
func buildIndexKey(name string, values ...string) string {
	return fmt.Sprintf("%s-%s", name, strings.Join(values, "_"))
}
