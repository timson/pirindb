package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/timson/pirindb/storage"
)

type ORM struct {
	db       *storage.DB
	metaOnce sync.Map
}

func New(db *storage.DB) *ORM {
	return &ORM{
		db: db,
	}
}

func (orm *ORM) getModelInfo(v any) (*ModelInfo, error) {
	name := reflect.TypeOf(v).String()
	if modelInfo, ok := orm.metaOnce.Load(name); ok {
		return modelInfo.(*ModelInfo), nil
	}
	modelInfo, err := parseModel(v)
	if err != nil {
		return nil, err
	}
	orm.metaOnce.Store(name, modelInfo)
	return modelInfo, nil
}

func (orm *ORM) Save(models ...any) error {
	if len(models) == 0 {
		return nil
	}

	err := orm.db.Update(func(tx *storage.Tx) error {
		for _, model := range models {
			modelInfo, err := orm.getModelInfo(model)
			if err != nil {
				return err
			}
			err = orm.saveOne(tx, modelInfo, model)
			if err != nil {
				return err
			}
		}

		return nil

	})

	return err
}

func (orm *ORM) saveOne(tx *storage.Tx, modelInfo *ModelInfo, model any) error {
	bucketName := modelInfo.BucketName()
	bucket, bucketErr := tx.CreateBucketIfNotExists([]byte(bucketName))
	if bucketErr != nil {
		return bucketErr
	}
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return fmt.Errorf("model pointer is nil")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct or pointer to struct")
	}

	pkField := rv.FieldByName(modelInfo.pk.fieldName)
	if !pkField.IsValid() {
		return fmt.Errorf("pk field %q not found", modelInfo.pk.fieldName)
	}
	var (
		pk  uint64
		err error
	)

	if modelInfo.pk.auto {
		// If auto PK is already set, treat Save as update.
		if pkField.Kind() == reflect.Uint64 && pkField.Uint() != 0 {
			pk = pkField.Uint()
		} else {
			pk, err = bucket.NextSequence()
			if err != nil {
				return err
			}
			if pkField.CanSet() && pkField.Kind() == reflect.Uint64 {
				pkField.SetUint(pk)
			}
		}
	} else {
		switch pkField.Kind() {
		case reflect.Uint64:
			pk = pkField.Uint()
		default:
			return fmt.Errorf("pk not uint64")
		}
	}
	if pk == 0 {
		return fmt.Errorf("pk must be greater than zero")
	}

	pkKey := encodeUint64BE(pk)
	oldData, existed := bucket.Get(pkKey)

	data, err := json.Marshal(rv.Interface())
	if err != nil {
		return err
	}
	err = bucket.Put(pkKey, data)
	if err != nil {
		return err
	}

	// If record existed, remove stale index entries before appending fresh ones.
	if existed && len(oldData) > 0 {
		oldPtr := reflect.New(rv.Type())
		if err = json.Unmarshal(oldData, oldPtr.Interface()); err != nil {
			return err
		}
		oldRV := oldPtr.Elem()
		for _, index := range modelInfo.indexes {
			indexBucketName := fmt.Sprintf("index/%s/%s", modelInfo.modelName, index.indexName)
			indexBucket, getBucketErr := tx.CreateBucketIfNotExists([]byte(indexBucketName))
			if getBucketErr != nil {
				return getBucketErr
			}
			oldValField := oldRV.FieldByName(index.fieldName)
			if !oldValField.IsValid() {
				continue
			}
			enc, encErr := encodeIndexValue(index.indexType, oldValField.Interface())
			if encErr != nil {
				return encErr
			}
			if removeErr := removePK(indexBucket, enc, pkKey); removeErr != nil {
				return removeErr
			}
		}
	}

	// handle indexes
	for _, index := range modelInfo.indexes {
		indexBucketName := fmt.Sprintf("index/%s/%s", modelInfo.modelName, index.indexName)
		var indexBucket *storage.Bucket
		indexBucket, err = tx.CreateBucketIfNotExists([]byte(indexBucketName))
		if err != nil {
			return err
		}
		val := rv.FieldByName(index.fieldName).Interface()
		enc, encErr := encodeIndexValue(index.indexType, val)
		if encErr != nil {
			return encErr
		}
		err = appendPK(indexBucket, enc, pkKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func appendPK(bucket *storage.Bucket, encIndexKey, pk []byte) error {
	fullKey := make([]byte, 0, len(encIndexKey)+1+len(pk))
	fullKey = append(fullKey, encIndexKey...)
	fullKey = append(fullKey, 0x1E)
	fullKey = append(fullKey, pk...)
	return bucket.Put(fullKey, nil)
}

func removePK(bucket *storage.Bucket, encIndexKey, pk []byte) error {
	fullKey := make([]byte, 0, len(encIndexKey)+1+len(pk))
	fullKey = append(fullKey, encIndexKey...)
	fullKey = append(fullKey, 0x1E)
	fullKey = append(fullKey, pk...)
	err := bucket.Remove(fullKey)
	if err == nil {
		return nil
	}
	if err == storage.ErrNodeNotFound {
		return nil
	}
	return err
}
