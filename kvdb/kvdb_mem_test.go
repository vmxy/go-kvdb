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

var tableMem Table[UserDemo]

func init() {
	InitMem(MemOptions{
		mem: true,
	})
	tableMem = NewTableMem[UserDemo]("userdemo")
}

func TestKVDB(t *testing.T) {
	fmt.Println("test===")
}
func TestInsert(t *testing.T) {
	fmt.Println("test===insert")
	for i := range 10 {
		user := UserDemo{
			ID:   fmt.Sprintf("%d", i),
			Name: "leo",
			Age:  11 + i,
			Addr: fmt.Sprintf("address no.%d", i),
		}
		if err := tableMem.Insert(user.ID, user); err != nil {
			fmt.Println("err===", err)
		}
	}
}
