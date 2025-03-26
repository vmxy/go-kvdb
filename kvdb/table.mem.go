package kvdb

import (
	"errors"
	"sync"

	"github.com/dgraph-io/ristretto"
)

type TableMem[T any] struct {
	//Table[T]
	name   string
	mdb    *ristretto.Cache
	idb    *ristretto.Cache
	keys   sync.Map
	indexs map[string]IndexInfo
}

var _ Table[string] = (*TableMem[string])(nil)

func NewTableMem[T any](name string) Table[T] {
	config := ristretto.Config{
		NumCounters: 1e7,
		MaxCost:     100,
		BufferItems: 64,
	}
	mdb, _ := ristretto.NewCache(&config)
	idb, _ := ristretto.NewCache(&config)
	table := TableMem[T]{
		name: name,
		mdb:  mdb,
		idb:  idb,
	}
	return &table
}

// Delete implements Table.
func (t *TableMem[T]) Delete(ids ...string) {
	for _, id := range ids {
		t.mdb.Del(id)
	}
}

// Get implements Table.
func (t *TableMem[T]) Get(id string) (v T, ok bool) {
	if v1, o1 := t.mdb.Get(id); o1 {
		if v, ok = v1.(T); ok {
			return v, ok
		} else {
			t.Delete(id)
		}
		return v, false
	}
	return v, false
}

// Gets implements Table.
func (t *TableMem[T]) Gets(ids ...string) (list []T) {
	panic("unimplemented")
}

// Insert implements Table.
func (t *TableMem[T]) Insert(id string, v T) error {
	if ok := t.mdb.Set(id, v, 0); !ok {
		return errors.New("insert fail")
	}
	return nil
}

// Name implements Table.
func (t *TableMem[T]) Name() string {
	panic("unimplemented")
}

// Scan implements Table.
func (t *TableMem[T]) Scan(handle func(t T) bool) {
	panic("unimplemented")
}

// Search implements Table.
func (t *TableMem[T]) Search(id string, filter func(t T) bool, start_end ...int) []T {
	panic("unimplemented")
}

// Update implements Table.
func (*TableMem[T]) Update(id string, t T) error {
	panic("unimplemented")
}
