package kvdb

import (
	"context"
	"fmt"

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

/*
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
*/
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
		if value, err = unmarshal[T](bs); err == nil {
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
			if ele, err := unmarshal[T](bs); err == nil {
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
		concatEntity(&eData, &entity)
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
