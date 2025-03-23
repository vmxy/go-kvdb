package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func buildIndexKey(name string, values ...string) string {
	return fmt.Sprintf("%s-%s", name, strings.Join(values, "_"))
}

type IndexInfo struct {
	Name  string
	Field string
	Type  string
}
type Table[T any] struct {
	Name   string
	mdb    *redis.Client
	idb    *redis.Client
	indexs map[string]IndexInfo
}

// 自定义 Logger，只输出 WARN 及以上级别的日志
type warnLogger struct{}

func (w warnLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func (w warnLogger) Warningf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func (w warnLogger) Infof(format string, args ...interface{}) {
	// INFO 级别的日志不输出
}

func (w warnLogger) Debugf(format string, args ...interface{}) {
	// DEBUG 级别的日志不输出
}
func GetHomeDir(dirs ...string) string {
	homeDir, err := os.Executable()
	if err == nil {
		homeDir = filepath.Dir(homeDir)
	}
	if err != nil {
		log.Fatalln("GetHomeDir error ", err)
	}
	if len(dirs) > 0 {
		return filepath.Join(homeDir, filepath.Join(dirs...))
	}
	return homeDir
}
func createMasterDB(table string) (*redis.Client, error) {
	return mdb, nil
}
func createIndexDB(table string) (*redis.Client, error) {
	return idb, nil
}

func NewTable[T any](name string) Table[T] {
	mdb, err := createMasterDB(name)
	if err != nil {
		log.Fatalf("create kvdb [%s] master error\r\n", name)
	}
	idb, err := createIndexDB(name)
	if err != nil {
		log.Fatalf("create kvdb [%s] index error\r\n", name)
	}
	table := Table[T]{
		Name:   name,
		mdb:    mdb,
		idb:    idb,
		indexs: make(map[string]IndexInfo),
	}
	table.createIndexs()
	table.onExit()
	return table
}
func (t *Table[T]) onExit() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		defer os.Exit(0)
		t.cleanup()
	}()
}
func (t *Table[T]) cleanup() {
	t.mdb.Close()
	t.idb.Close()
}

func (t *Table[T]) createIndexs() {
	mode := new(T)
	modeType := getRefTypeElem(mode)
	var indexes []IndexInfo
	for i := range modeType.NumField() {
		field := modeType.Field(i)
		tag := field.Tag.Get("gorm")
		if strings.Contains(tag, "primaryKey") {
		} else if strings.Contains(tag, "index:") {
			indexName := strings.Split(tag, "index:")[1]
			indexName = regexp.MustCompile(`;.*$`).ReplaceAllString(indexName, "")
			indexInfo := IndexInfo{
				Name:  indexName,
				Field: field.Name,
				Type:  field.Type.String(),
			}
			indexes = append(indexes, indexInfo)
		} else if strings.Contains(tag, "uniqueIndex:") {
			indexName := strings.Split(tag, "uniqueIndex:")[1]
			indexName = regexp.MustCompile(`;.*$`).ReplaceAllString(indexName, "")
			indexInfo := IndexInfo{
				Name:  indexName,
				Field: field.Name,
				Type:  field.Type.String(),
			}
			indexes = append(indexes, indexInfo)
		}
	}
	for _, idx := range indexes {
		t.indexs[idx.Name] = idx
	}

}

func (t *Table[T]) Get(id string) (value T, ok bool) {
	v, err := t.mdb.Get(ctx, id).Result()
	if err != nil {
		return value, false
	}
	if v == "" {
		t.mdb.Del(ctx, id)
		return value, false
	}
	value, err = to[T]([]byte(v))
	return value, ok
}

func (t *Table[T]) SearchByIdx(idxname string, value any, filter func(t T) bool, startAndEnd ...int) []T {
	if i, ok := t.indexs[idxname]; ok {
		var start, end int = 0, 1
		if len(startAndEnd) == 1 {
			start = startAndEnd[0]
			end = start + 1
		}
		key := buildIndexKey(i.Name, fmt.Sprintf("%v", value))
		return t.search(key, false, start, end, filter)
	}
	return make([]T, 0)
}
func (t *Table[T]) Search(id string, filter func(t T) bool, startAndEnd ...int) []T {
	var start, end int = 0, 1
	if len(startAndEnd) == 1 {
		start = startAndEnd[0]
		end = start + 1
	} else if len(startAndEnd) >= 2 {
		start = startAndEnd[0]
		end = startAndEnd[1] + 1
	}
	return t.search(id, true, start, end, filter)
}
func (t *Table[T]) Seek(handle func(v T)) {
	ctx := context.Background()
	// 初始化游标
	var cursor uint64
	var keys []string
	// 使用 SCAN 遍历所有键
	for {
		var err error
		keys, cursor, err = t.mdb.Scan(ctx, cursor, "*", 100).Result()
		if err != nil {
			log.Println("scan error", err)
			break
		}
		// 处理匹配的键
		for _, key := range keys {
			if val, ok := t.Get(key); ok {
				handle(val)
			}
		}
		// 如果游标为 0，表示遍历结束
		if cursor == 0 {
			break
		}
	}
}

type KVS struct {
	K []string
	V []any
}

func (t *Table[T]) search(key string, isMain bool, start int, end int, filter func(t T) bool) (list []T) {
	defer func() {
		if r := recover(); r != nil {
			list = make([]T, 0)
			debug.PrintStack()
		}
	}()

	var totalSize = end - start
	var db *redis.Client = t.mdb
	if !isMain {
		db = t.idb
	}
	ctx := context.Background()
	// 初始化游标
	var cursor uint64 = uint64(start)
	var keys []string
	var curIdx int = 0
	// 使用 SCAN 遍历所有键
	for {
		if len(list) >= totalSize {
			break
		}
		var err error
		keys, cursor, err = db.Scan(ctx, cursor, "*", 10).Result()
		if err != nil {
			log.Println("scan error", err)
			break
		}
		var kv KVS

		if isMain {
			values, err := db.MGet(ctx, keys...).Result()
			if err != nil {
				return list
			}
			kv = KVS{
				K: keys,
				V: values,
			}
		} else {
			ids0, err := db.MGet(ctx, keys...).Result()
			if err != nil {
				return list
			}
			var ids []string
			for _, id := range ids0 {
				ids = append(ids, fmt.Sprintf("%v", id))
			}
			values, err := t.mdb.MGet(ctx, ids...).Result()
			kv = KVS{
				K: ids,
				V: values,
			}
		}
		//var dels []string
		for i, val := range kv.V {
			if len(list) >= totalSize {
				return
			}
			skey := kv.K[i]
			if !strings.HasPrefix(skey, key) {
				return
			}
			if start > curIdx {
				curIdx++
				continue
			}
			if curIdx >= end {
				return
			}
			if v, ok := val.(string); ok {
				x, err := to[T]([]byte(v))
				if err != nil {
					continue
				}
				status := filter(x)
				if !status {
					continue
				}
				curIdx++
				list = append(list, x)
			}
		}
		// 如果游标为 0，表示遍历结束
		if cursor == 0 {
			break
		}
	}

	return list
}

func (t *Table[T]) Update(id any, entity T) (err error) {
	uid := fmt.Sprintf("%v", id)
	eData, exist := t.Get(uid)
	rentity := getRefValueElem(entity)
	ctx := context.Background()
	ipipe := t.idb.Pipeline()
	for _, idx := range t.indexs {
		value := rentity.FieldByName(idx.Field)
		if !value.IsZero() {
			key := buildIndexKey(idx.Name, value.String(), uid)

			ipipe.Set(ctx, key, uid, 0)
			if oldVal := getValue(eData, idx.Field); oldVal.IsValid() && !oldVal.IsZero() {
				key := buildIndexKey(idx.Name, oldVal.String(), uid)
				ipipe.Del(ctx, key)
			}
		}
	}
	ipipe.Exec(ctx)
	if exist {
		concat(&eData, &entity)
	}

	if json, err := json.Marshal(entity); err == nil {
		_, err = t.mdb.Set(ctx, uid, json, 0).Result()
		return err
	}
	return err
}
func (t *Table[T]) Delete(id any) (err error) {
	ctx := context.Background()
	uid := fmt.Sprintf("%v", id)
	if entity, ok := t.Get(uid); ok {
		rentity := getRefValueElem(entity)
		ipipe := t.idb.Pipeline()
		for _, idx := range t.indexs {
			value := rentity.FieldByName(idx.Name)
			key := buildIndexKey(idx.Name, value.String())
			ipipe.Del(ctx, key)
		}
		ipipe.Exec(ctx)
		t.mdb.Del(context.Background(), uid)
	}
	return nil
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
	err = json.Unmarshal(val, &obj)
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
