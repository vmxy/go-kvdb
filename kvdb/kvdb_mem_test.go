package kvdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type UserDemo struct {
	ID    string
	Name  string `kvdb:"index:idx_name"`
	Age   int
	Addr  string `kvdb:"index:idx_addr|r"`
	OK    bool
	Count int64
}

func (u *UserDemo) String() string {
	return fmt.Sprintf("ID=%s Name=%s Age=%d Addr=%s OK=%t Count=%d", u.ID, u.Name, u.Age, u.Addr, u.OK, u.Count)
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
		Mem: true,
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
	// 正确做法 - 初始化 map
	m := H{"Age": 11}
	m["Name"] = "xx"
	_, err := marshal(m)
	fmt.Println("============err", err)
	user := UserDemo{
		ID:    "911",
		Name:  "leo",
		Age:   11,
		Addr:  "address no.911",
		OK:    false,
		Count: 33,
	}
	if err := tableMem.Insert(user.ID, &user); err != nil {
		fmt.Println("err===", err)
	}
	u, ok := tableMem.Get(user.ID)
	fmt.Println("get1===", u.ID, u.Name, ok, u)

	tableMem.Update(user.ID, H{"Name": "", "Age": 19})

	u, ok = tableMem.Get(user.ID)
	fmt.Println("get2===", u.ID, u.Name, ok, u)

	for i := range 20 {
		name := fmt.Sprintf("leox%d", i)
		if i%10 == 1 {
			name = ""
		}
		user := UserDemo{
			ID:   fmt.Sprintf("%d", i),
			Name: name,
			Age:  11 + i,
			Addr: fmt.Sprintf("address no.%d", i),
		}
		if err := tableMem.Insert(user.ID, &user); err != nil {
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
	for i, v := range tableMem.SearchByIdx("idx_name", "leo", func(v UserDemo) bool { return v.Age%2 == 0 }, 0, 5) {
		fmt.Println("test search idx ", i, v)
	}
	for i, v := range tableMem.SearchByIdx("idx_name", "", func(v UserDemo) bool { return v.Age%2 == 0 }, 0, 5) {
		fmt.Println("test search idx empty==> ", i, v)
	}
	fmt.Println("================== test update =================")
	ids := []string{"2", "8", "5", "9"}
	///
	for i, v := range tableMem.Gets(ids...) {
		fmt.Println("test update1 ", v)
		addr := v.Addr
		if i == 0 {
			addr = ""
		}
		x := tableMem.Update(v.ID, H{
			"Name": fmt.Sprintf("liming-%d", (i * 3)),
			"Addr": addr,
		})
		v2, ok := tableMem.Get(v.ID)
		fmt.Println("test update2 ", v2, ok, x)
	}
	time.Sleep(1 * time.Second)
	if v, ok := tableMem.Get("2"); ok {
		fmt.Println("v1 ", v.String())
		tableMem.Update(v.ID, H{"Name": ""})
		time.Sleep(1 * time.Second)
		for i, x := range tableMem.SearchByIdx("idx_name", "", func(v UserDemo) bool { return true }, 0, 10) {
			fmt.Println("v2 get ", i, x.String())
		}

		tableMem.Update(v.ID, H{"Name": "33"})
		time.Sleep(1 * time.Second)
		for i, x := range tableMem.SearchByIdx("idx_name", "", func(v UserDemo) bool { return true }, 0, 10) {
			fmt.Println("v3 get ", i, x.String())
		}
	}
}
