package orm

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type idxType int

const (
	idxString idxType = iota + 1
	idxInt
	idxTimestamp
	idxBool
	idxComposite
)

type FieldInfo struct {
	fieldName string
	indexName string
	indexType idxType
	composite []string
	isPK      bool
	auto      bool
}

type ModelInfo struct {
	modelName  string
	pk         FieldInfo
	indexes    []FieldInfo
	composites []FieldInfo
	byField    map[string]FieldInfo
}

func (mi *ModelInfo) BucketName() string {
	return fmt.Sprintf("model/%s", mi.modelName)
}

func readTag(sf reflect.StructField, tag string) (FieldInfo, error) {
	fieldInfo := FieldInfo{
		fieldName: sf.Name,
	}
	for _, p := range strings.Split(tag, ",") {
		p = strings.TrimSpace(p)
		switch {
		case p == "pk":
			fieldInfo.isPK = true
		case p == "auto":
			fieldInfo.auto = true
		case strings.HasPrefix(p, "idx="):
			fieldInfo.indexName = strings.TrimPrefix(p, "idx=")
		case strings.HasPrefix(p, "composite="):
			fieldInfo.indexType = idxComposite
			fieldInfo.composite = strings.Split(strings.TrimPrefix(p, "composite="), "|")
		case p == "string":
			fieldInfo.indexType = idxString
		case p == "int":
			fieldInfo.indexType = idxInt
		case p == "ts":
			fieldInfo.indexType = idxTimestamp
		case p == "bool":
			fieldInfo.indexType = idxBool
		}
	}

	if fieldInfo.isPK {
		k := sf.Type.Kind()
		if k != reflect.Uint64 {
			return fieldInfo, fmt.Errorf("primary key must be an uint64, got %s", k)
		}
	}

	if fieldInfo.indexType == 0 && !fieldInfo.isPK {
		switch sf.Type.Kind() {
		case reflect.String:
			fieldInfo.indexType = idxString
		case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
			fieldInfo.indexType = idxInt
		case reflect.Bool:
			fieldInfo.indexType = idxBool
		default:
			return FieldInfo{}, fmt.Errorf("invalid index type %s", sf.Type.Kind())
		}
	}

	if fieldInfo.indexName == "" && !fieldInfo.isPK && fieldInfo.indexType != idxComposite {
		fieldInfo.indexName = strings.ToLower(sf.Name)
	}

	return fieldInfo, nil
}

func parseModel(model any) (*ModelInfo, error) {
	rt := reflect.TypeOf(model)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct, got %s", rt.Kind())
	}

	modelInfo := &ModelInfo{
		modelName: rt.Name(),
		byField:   make(map[string]FieldInfo),
	}

	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		tag := sf.Tag.Get("pirin")
		if tag == "" {
			continue
		}
		fieldInfo, err := readTag(sf, tag)
		if err != nil {
			return nil, fmt.Errorf("invalid tag '%s': %s", tag, err)
		}

		switch {
		case fieldInfo.isPK:
			if modelInfo.pk.isPK {
				return nil, fmt.Errorf("multiple PK fields are not supported")
			}
			modelInfo.pk = fieldInfo
		case fieldInfo.indexType == idxComposite:
			modelInfo.composites = append(modelInfo.composites, fieldInfo)
		default:
			modelInfo.indexes = append(modelInfo.indexes, fieldInfo)
		}
		modelInfo.byField[sf.Name] = fieldInfo
		modelInfo.byField[strings.ToLower(sf.Name)] = fieldInfo
		if fieldInfo.indexName != "" {
			modelInfo.byField[fieldInfo.indexName] = fieldInfo
			modelInfo.byField[strings.ToLower(fieldInfo.indexName)] = fieldInfo
		}

	}

	if !modelInfo.pk.isPK {
		return nil, fmt.Errorf("model does not have a PK field")
	}

	return modelInfo, nil
}

func encodeUint64BE(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func encodeIndexValue(indexType idxType, v any) ([]byte, error) {
	switch indexType {
	case idxString:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string type for idxString, got %T", v)
		}
		return []byte(s), nil
	case idxInt:
		var u uint64
		switch n := v.(type) {
		case int:
			u = uint64(n)
		case int64:
			u = uint64(n)
		case int32:
			u = uint64(n)
		case uint:
			u = uint64(n)
		case uint64:
			u = n
		case uint32:
			u = uint64(n)
		default:
			return nil, fmt.Errorf("expected integer type for idxInt, got %T", v)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], u)
		return buf[:], nil
	case idxTimestamp:
		var buf [8]byte
		t, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected time.Time type for idxTimestamp, got %T", v)
		}
		binary.BigEndian.PutUint64(buf[:], uint64(t.UnixNano()))
		return buf[:], nil
	case idxBool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool type for idxBool, got %T", v)
		}
		if b {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	default:
		return nil, fmt.Errorf("invalid index type %d", indexType)
	}
}

func (mi *ModelInfo) LookupField(name string) (FieldInfo, bool) {
	if mi == nil {
		return FieldInfo{}, false
	}
	fi, ok := mi.byField[name]
	if ok {
		return fi, true
	}
	fi, ok = mi.byField[strings.ToLower(name)]
	return fi, ok
}
