package kvdb

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"runtime/debug"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type TableRedis[T any] struct {
	name   string
	mdb    *redis.Client
	idb    *redis.Client
	indexs map[string]IndexInfo
}

// 自定义 Logger，只输出 WARN 及以上级别的日志
/* type warnLogger struct{}

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
*/
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
	table := TableRedis[T]{
		name:   name,
		mdb:    mdb,
		idb:    idb,
		indexs: make(map[string]IndexInfo),
	}
	table.createIndexs()
	//table.onExit()
	return &table
}
func (t *TableRedis[T]) Name() string {
	return t.name
}

/* func (t *Table[T]) onExit() {
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
} */

func (t *TableRedis[T]) createIndexs() {
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

func (t *TableRedis[T]) Get(id string) (value T, ok bool) {
	cmd := t.mdb.Get(context.Background(), id)
	if err := cmd.Err(); err != nil {
		if err != redis.Nil {
			t.mdb.Del(context.Background(), id)
		}
		return value, false
	}
	if bs, err := cmd.Bytes(); err == nil {
		if len(bs) < 1 {
			t.mdb.Del(context.Background(), id)
			return value, false
		}
		if value, err = to[T](bs); err == nil {
			return value, true
		} else {
			t.mdb.Del(context.Background(), id)
			return value, false
		}
	} else {
		t.mdb.Del(context.Background(), id)
	}
	return value, false
}
func (t *TableRedis[T]) Gets(ids ...string) (list []T) {
	vs, err := t.mdb.MGet(context.Background(), ids...).Result()
	if err != nil {
		return list
	}
	var delIds []string
	for i, v := range vs {
		if bs, ok := v.([]byte); ok {
			if ele, err := to[T](bs); err == nil {
				list = append(list, ele)
			} else {
				delIds = append(delIds, ids[i])
			}
		} else {
			delIds = append(delIds, ids[i])
		}
	}
	t.Delete(delIds...)
	return list
}

func (t *TableRedis[T]) Insert(id string, entity T) (err error) {
	uid := fmt.Sprintf("%v", id)
	rentity := getRefValueElem(entity)
	ctx := context.Background()
	ipipe := t.idb.Pipeline()
	for _, idx := range t.indexs {
		value := rentity.FieldByName(idx.Field)
		if !value.IsZero() {
			key := buildIndexKey(idx.Name, value.String(), uid)
			ipipe.Set(ctx, key, uid, 0)
		}
	}
	ipipe.Exec(ctx)
	if bs, err := msgpack.Marshal(entity); err == nil {
		_, err = t.mdb.Set(ctx, uid, bs, 0).Result()
		return err
	}
	<-ctx.Done()
	return err
}

func (t *TableRedis[T]) Update(id string, entity T) (err error) {
	uid := fmt.Sprintf("%v", id)
	eData, exist := t.Get(uid)
	if !exist {
		return fmt.Errorf("update id=%v is noexist", id)
	}
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
	if json, err := msgpack.Marshal(entity); err == nil {
		_, err = t.mdb.Set(ctx, uid, json, 0).Result()
		return err
	}
	return err
}
func (t *TableRedis[T]) Delete(id ...string) {
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
		if _, err := ipipe.Exec(ctx); err == nil {
			//<-ctx.Done()
		}
		t.mdb.Del(context.Background(), uid).Result()
	}
}
func (t *TableRedis[T]) SearchByIdx(idxname string, value any, filter func(t T) bool, startAndEnd ...int) []T {
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
func (t *TableRedis[T]) Search(id string, filter func(t T) bool, startAndEnd ...int) []T {
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
func (t *TableRedis[T]) Scan(handle func(v T) bool) {
	// 初始化游标
	var cursor uint64
	var keys []string
	// 使用 SCAN 遍历所有键
	var isEnd = false
	for {
		if isEnd {
			break
		}
		var err error
		keys, cursor, err = t.mdb.Scan(context.Background(), cursor, "*", 100).Result()
		if err != nil {
			log.Println("scan error", err)
			break
		}
		var delIds []string
		if vs, err := t.mdb.MGet(context.Background(), keys...).Result(); err == nil && len(vs) > 0 {
			for i, v := range vs {
				if bs, ok := v.([]byte); ok {
					ele, _ := to[T](bs)
					isEnd = handle(ele)
					if isEnd {
						break
					}
				} else {
					delIds = append(delIds, keys[i])
				}
			}
		}
		if len(delIds) > 0 {
			t.Delete(delIds...)
		}
		// 如果游标为 0，表示遍历结束
		if cursor == 0 {
			isEnd = true
			break
		}
	}
}

type KVS struct {
	K []string
	V []any
}

func (t *TableRedis[T]) search(key string, isMain bool, start int, end int, filter func(t T) bool) (list []T) {
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
