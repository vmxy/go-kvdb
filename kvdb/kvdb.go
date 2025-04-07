package kvdb

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type IndexInfo struct {
	Name  string
	Field string
	Type  string
}
type Entity interface {
}
type Table[T Entity] interface {
	Name() string //表名
	//createIndexs() //创建索引
	//Name   string
	//mdb    *redis.Client
	//idb    *redis.Client
	//Indexs() map[string]IndexInfo
	Get(id string) (v T, ok bool)                                                          //获取,根据id
	Gets(ids ...string) (list []T)                                                         //获取列表,多个id
	Insert(id string, v *T) error                                                          //插入
	Update(id string, v H) error                                                           //更新
	Delete(ids ...string)                                                                  //删除
	Search(id string, filter func(v T) bool, start_end ...int) (list []T)                  //搜索
	SearchByIdx(idx string, value any, filter func(v T) bool, start_end ...int) (list []T) //搜索
	Scan(handle func(v T) bool)
	Close() //扫描
	init()  //初始化db表
}

func NewTable[T Entity](name string) Table[T] {
	table := NewTableMem[T](name)
	return table
}

func createIndexs[T any]() map[string]IndexInfo {
	mapidxs := make(map[string]IndexInfo)
	mode := new(T)
	modeType := getRefTypeElem(mode)
	var indexes []IndexInfo
	for i := range modeType.NumField() {
		field := modeType.Field(i)
		tag := field.Tag.Get("kvdb")
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
		} /* else if strings.Contains(tag, "uniqueIndex:") {
			indexName := strings.Split(tag, "uniqueIndex:")[1]
			indexName = regexp.MustCompile(`;.*$`).ReplaceAllString(indexName, "")
			indexInfo := IndexInfo{
				Name:  indexName,
				Field: field.Name,
				Type:  field.Type.String(),
			}
			indexes = append(indexes, indexInfo)
		} */
	}
	for _, idx := range indexes {
		mapidxs[idx.Name] = idx
	}
	return mapidxs
}
func concatEntity[T any](oldVal T, newVal H) H {
	rstruct := getRefTypeElem(oldVal)

	//rvalue := getRefValueElem(newVal)
	for i := range rstruct.NumField() {
		field := rstruct.Field(i)
		/* valueNew := rvalue.FieldByName(field.Name)
		oldValue := getValue(oldVal, field.Name)
		if valueNew.IsZero() && valueNew.CanSet() {
			valueNew.IsValid()
			valueNew.Set(oldValue)
		} */
		oldValue := getValue(oldVal, field.Name)
		if _, ok := newVal[field.Name]; !ok {
			newVal[field.Name] = getTypeRealValue(oldValue)
		}
	}
	return newVal
}
func getTypeRealValue(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		return v.String()
	default:
		return v.Interface()
	}
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
func isSameValueRef(v1 reflect.Value, v2 reflect.Value) bool {
	if v1.Type() != v2.Type() {
		return false
	}
	return reflect.DeepEqual(v1.Interface(), v2.Interface())
}
func isSameValue(v1 any, v2 reflect.Value) bool {
	return reflect.DeepEqual(v1, v2.Interface())
}
func buildIndexKey(name string, values ...string) string {
	return fmt.Sprintf("%s-%s", name, strings.Join(values, "_"))
}

// H is a shortcut for map[string]any
type H map[string]any
