package orm

import (
	"fmt"
	"reflect"
)

// TypedModel provides typed query helpers on top of ORM.
// T must be a struct type (not pointer).
type TypedModel[T any] struct {
	orm *ORM
}

func Model[T any](orm *ORM) *TypedModel[T] {
	return &TypedModel[T]{orm: orm}
}

func FindAll[T any](orm *ORM, q *Query) ([]*T, error) {
	return Model[T](orm).FindAll(q)
}

func FindOne[T any](orm *ORM, q *Query) (*T, error) {
	return Model[T](orm).FindOne(q)
}

func (m *TypedModel[T]) FindAll(q *Query) ([]*T, error) {
	if m == nil || m.orm == nil {
		return nil, fmt.Errorf("orm is nil")
	}
	proto, err := modelPrototype[T]()
	if err != nil {
		return nil, err
	}

	rows, err := m.orm.Find(proto, q)
	if err != nil {
		return nil, err
	}

	out := make([]*T, 0, len(rows))
	for _, row := range rows {
		switch v := row.(type) {
		case *T:
			out = append(out, v)
		case T:
			rowCopy := v
			out = append(out, &rowCopy)
		default:
			return nil, fmt.Errorf("unexpected model type %T in typed result", row)
		}
	}
	return out, nil
}

func (m *TypedModel[T]) FindOne(q *Query) (*T, error) {
	rows, err := m.FindAll(q)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	if len(rows) > 1 {
		return nil, ErrMultipleResults
	}
	return rows[0], nil
}

func modelPrototype[T any]() (any, error) {
	rt := reflect.TypeOf((*T)(nil)).Elem()
	if rt.Kind() == reflect.Pointer {
		return nil, fmt.Errorf("T must be a non-pointer struct type")
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("T must be a struct type, got %s", rt.Kind())
	}
	return reflect.New(rt).Elem().Interface(), nil
}
