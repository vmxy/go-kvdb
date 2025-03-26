package kvdb

import (
	"fmt"
	"testing"
	"time"
)

type UserDemo struct {
	ID   string
	Name string `kvdb:"index:idx_name"`
	Age  int
	Addr string
}

func initdb() Table[UserDemo] {
	InitMem(MemOptions{
		mem: true,
	})
	return NewTableMem[UserDemo]("userdemo")
}

func TestMem(t *testing.T) {
	fmt.Println("test===")
}
func TestMemInsert(t *testing.T) {
	var tableMem Table[UserDemo] = initdb()
	fmt.Println("test===insert")
	for i := range 100 {
		user := UserDemo{
			ID:   fmt.Sprintf("%d", i),
			Name: "leo",
			Age:  11 + i,
			Addr: fmt.Sprintf("address no.%d", i),
		}
		if err := tableMem.Insert(user.ID, user); err != nil {
			fmt.Println("err===", err)
		}
		//g, o := tableMem.Get(user.ID)
		//fmt.Println("test got ", g, o)
	}
	time.Sleep(1 * time.Second)
	///
	for i := range 10 {
		if v, ok := tableMem.Get(fmt.Sprintf("%d", i)); ok {
			fmt.Println("test get ", i, v)
		}
	}
	///
	for i, v := range tableMem.Gets("1", "3", "7", "9") {
		fmt.Println("test gets ", i, v)
	}

	//
	for i, v := range tableMem.Search("3", func(v UserDemo) bool { return v.Age < 46 }, 0, 10) {
		fmt.Println("test search main ", i, v)
	}

	//
	for i, v := range tableMem.SearchByIdx("idx_name", "leo", func(v UserDemo) bool { return v.Age < 50 && v.Age >= 40 }, 0, 10) {
		fmt.Println("test search idx ", i, v)
	}
}
