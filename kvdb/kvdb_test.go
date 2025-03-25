package kvdb

import (
	"fmt"
	"testing"
)

type UserDemo struct {
	ID   string
	Name string
	Age  int
	Addr string
}

var table Table[UserDemo]

func init() {
	InitRedis(Options{
		Host:     "127.0.0.1",
		Port:     6371,
		Password: "123456",
		DB:       2,
	})
	table = NewTable[UserDemo]("userdemo")
}

func TestKVDB(t *testing.T) {
	fmt.Println("test===")
}
func TestInsert(t *testing.T) {
	fmt.Println("test===insert")
	user := UserDemo{
		ID:   "1",
		Name: "leo",
		Age:  11,
		Addr: "address no.1",
	}
	err := table.Insert(user.ID, user)
	fmt.Println("err===", err)
}
