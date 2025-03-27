package kvdb

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/ristretto"
)

var config = ristretto.Config{
	NumCounters: 1e7,
	MaxCost:     1000,
	BufferItems: 64,
}

//var cache, _ = ristretto.NewCache(&config)

type TableMem[T Entity] struct {
	//Table[T]
	name   string
	mdb    *pebble.DB
	idb    *pebble.DB
	cache  *ristretto.Cache
	indexs map[string]IndexInfo
}

var _ Table[Entity] = (*TableMem[Entity])(nil)
var writerOpt = pebble.WriteOptions{Sync: true}

func NewTableMem[T Entity](name string) Table[T] {
	cache, _ := ristretto.NewCache(&config)
	table := TableMem[T]{
		name:   name,
		mdb:    memMdb,
		idb:    memIdb,
		cache:  cache,
		indexs: createIndexs[T](),
	}
	fmt.Println("create idxs", table.indexs)
	return &table
}

// Name implements Table.
func (t *TableMem[T]) Name() string {
	return t.name
}

// Get implements Table.
func (t *TableMem[T]) Get(id string) (v T, ok bool) {
	if v1, o1 := t.cache.Get(id); o1 {
		if v, ok = v1.(T); ok {
			return v, ok
		} else {
			t.cache.Del(id)
		}
	}
	if bs, closer, err := t.mdb.Get([]byte(id)); err == nil {
		defer closer.Close()
		if v, err := unmarshal[T](bs); err == nil {
			t.cache.Set(id, v, 1)
			//fmt.Println("get ", id, v)
			return v, true
		} else {
			t.Delete(id)
		}
		return v, false
	}
	return v, false
}

// Gets implements Table.
func (t *TableMem[T]) Gets(ids ...string) (list []T) {
	for _, id := range ids {
		if v, ok := t.Get(id); ok {
			list = append(list, v)
		}
	}
	return list
}

// Insert implements Table.
func (t *TableMem[T]) Insert(id string, v *T) error {
	if json, err := marshal(v); err == nil {
		if e1 := t.mdb.Set([]byte(id), json, &writerOpt); e1 == nil {
			rentity := getRefValueElem(v)
			for _, idx := range t.indexs {
				value := rentity.FieldByName(idx.Field)
				if !value.IsZero() {
					key := buildIndexKey(idx.Name, value.String(), id)
					t.idb.Set([]byte(key), []byte(id), pebble.Sync)
				}
			}
			return nil
		} else {
			return e1
		}
	} else {
		return err
	}
}

// Update implements Table.
func (t *TableMem[T]) Update(id string, v *T) error {
	if o, ok := t.Get(id); ok {
		rentity := getRefValueElem(v)
		for _, idx := range t.indexs {
			value := rentity.FieldByName(idx.Field)
			if !value.IsZero() {
				key := buildIndexKey(idx.Name, value.String(), id)
				t.idb.Set([]byte(key), []byte(id), &writerOpt)
				if oldVal := getValue(o, idx.Field); oldVal.IsValid() && !oldVal.IsZero() {
					key := buildIndexKey(idx.Name, oldVal.String(), id)
					t.idb.Delete([]byte(key), pebble.Sync)
				}
			}
		}
		concatEntity[*T](&o, v)
		if json, err := marshal(v); err == nil {
			t.mdb.Set([]byte(id), json, pebble.Sync)
			if _, ok := t.cache.Get(id); ok {
				t.cache.Set(id, v, 0)
			}
			return nil
		} else {
			return err
		}
	}
	return errors.New("exist " + id)
}

// Delete implements Table.
func (t *TableMem[T]) Delete(ids ...string) {
	for _, id := range ids {
		if v, ok := t.Get(id); ok {
			rentity := getRefValueElem(v)
			for _, idx := range t.indexs {
				value := rentity.FieldByName(idx.Name)
				key := buildIndexKey(idx.Name, value.String())
				//itxn.Delete([]byte(key))
				t.idb.Delete([]byte(key), pebble.Sync)
			}
		}
		t.cache.Del(id)
		t.mdb.Delete([]byte(id), pebble.Sync)
	}
}

// Search implements Table.
func (t *TableMem[T]) Search(key string, filter func(t T) bool, start_end ...int) (list []T) {
	return t.search(true, key, filter, start_end...)
}

// SearchByIdx implements Table.
func (t *TableMem[T]) SearchByIdx(idxname string, value any, filter func(t T) bool, start_end ...int) (list []T) {
	if i, ok := t.indexs[idxname]; ok {
		key := buildIndexKey(i.Name, fmt.Sprintf("%v", value))
		return t.search(false, key, filter, start_end...)
	}
	return make([]T, 0)
}

// Search implements Table.
func (t *TableMem[T]) search(isMain bool, key string, filter func(t T) bool, start_end ...int) (list []T) {
	var start, end int = 0, 10
	if len(start_end) == 1 {
		start = start_end[0]
		if start < 0 {
			start = 0
		}
	}
	if len(start_end) >= 2 {
		end = start_end[1]
		if end < start {
			end = start + 1
		}
	}
	size := end - start
	t.scan(isMain, &pebble.IterOptions{
		LowerBound: []byte(fmt.Sprintf("%s", key)),
		UpperBound: []byte(fmt.Sprintf("%s\xff", key)),
	}, func(v T) bool {
		if filter(v) {
			list = append(list, v)
		}
		if len(list) >= size {
			return false
		}
		return true
	})
	return list
}

// Scan implements Table.
func (t *TableMem[T]) Scan(handle func(v T) bool) {
	t.scan(true, nil, handle)
}
func (t *TableMem[T]) scan(isMain bool, op *pebble.IterOptions, handle func(v T) bool) {
	var db *pebble.DB = is(isMain, t.mdb, t.idb)
	// 遍历所有键值
	iter, _ := db.NewIter(op)
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var id string = is(isMain, string(iter.Key()), string(iter.Value()))
		if v, ok := t.cache.Get(id); ok {
			if v2, o2 := v.(T); o2 {
				if o := handle(v2); o {
					continue
				} else {
					break
				}
			} else {
				t.cache.Del(id)
			}
		}
		if isMain {
			if v, err := unmarshal[T](iter.Value()); err == nil {
				if o := handle(v); o {
					continue
				} else {
					break
				}
			}
		} else {
			if v, ok := t.Get(id); ok {
				if o := handle(v); o {
					continue
				} else {
					break
				}
			}
		}
	}
}
