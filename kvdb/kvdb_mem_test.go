package kvdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type UserDemo struct {
	ID   string
	Name string `kvdb:"index:idx_name"`
	Age  int
	Addr string
}

func getAppDir(dirs ...string) string {
	var homeDir = ""
	if wd, err := os.Getwd(); err == nil {
		homeDir = filepath.Join(wd, "../")
	}
	if homeDir == "" {
		if d, err := os.Executable(); err == nil {
			homeDir = filepath.Dir(d)
		}
	}
	if len(dirs) > 0 {
		return filepath.Join(homeDir, filepath.Join(dirs...))
	}
	return homeDir
}
func initdb() Table[UserDemo] {
	dir := getAppDir("data")
	fmt.Println("dir", dir)
	InitMem(MemOptions{
		mem: true,
		//dir: dir,
	})
	return NewTableMem[UserDemo]("userdemo")
}

func TestMem(t *testing.T) {
	fmt.Println("test===")
}
func TestMemInsert(t *testing.T) {
	var tableMem = initdb()
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
	for i, v := range tableMem.SearchByIdx("idx_name", "leo", func(v UserDemo) bool { return v.Age%2 == 0 }, 27, 20) {
		fmt.Println("test search idx ", i, v)
	}

	fmt.Println("================== test update =================")
	ids := []string{"2", "8", "5", "9"}
	///
	for i, v := range tableMem.Gets(ids...) {
		fmt.Println("test update1 ", v)
		x := tableMem.Update(v.ID, UserDemo{
			Name: fmt.Sprintf("liming-%d", (i * 3)),
		})
		v2, ok := tableMem.Get(v.ID)
		fmt.Println("test update2 ", v2, ok, x)

	}

}
