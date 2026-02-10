package orm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/timson/pirindb/storage"
)

const indexPKSeparator = byte(0x1E)

type queryNode interface {
	isNode()
}

type pred struct {
	field string
	op    string
	val   any
}

func (*pred) isNode() {}

type conj struct {
	left  queryNode
	right queryNode
	op    string
}

func (*conj) isNode() {}

type neg struct {
	inner queryNode
}

func (*neg) isNode() {}

type Query struct {
	root queryNode
	err  error
	// limit < 0 means unlimited.
	limit  int
	offset int
}

func Where(field string) *Query {
	if field == "" {
		return &Query{err: fmt.Errorf("field is empty"), limit: -1}
	}
	return &Query{root: &pred{field: field}, limit: -1}
}

func (q *Query) Eq(v any) *Query {
	return q.setPred("=", v)
}

func (q *Query) Ne(v any) *Query {
	return q.setPred("!=", v)
}

func (q *Query) Gt(v any) *Query {
	return q.setPred(">", v)
}

func (q *Query) Gte(v any) *Query {
	return q.setPred(">=", v)
}

func (q *Query) Lt(v any) *Query {
	return q.setPred("<", v)
}

func (q *Query) Lte(v any) *Query {
	return q.setPred("<=", v)
}

func (q *Query) setPred(op string, v any) *Query {
	if q == nil {
		return &Query{err: fmt.Errorf("query is nil"), limit: -1}
	}
	if q.err != nil {
		return q
	}
	p, ok := q.root.(*pred)
	if !ok || p == nil {
		q.err = fmt.Errorf("operator %s can only be applied directly after Where()", op)
		return q
	}
	if p.op != "" {
		q.err = fmt.Errorf("predicate for field %q is already defined", p.field)
		return q
	}
	p.op = op
	p.val = v
	return q
}

func (q *Query) Limit(n int) *Query {
	if q == nil {
		return &Query{err: fmt.Errorf("query is nil"), limit: -1}
	}
	if q.err != nil {
		return q
	}
	if n < 0 {
		q.err = fmt.Errorf("limit must be >= 0")
		return q
	}
	q.limit = n
	return q
}

func (q *Query) Offset(n int) *Query {
	if q == nil {
		return &Query{err: fmt.Errorf("query is nil"), limit: -1}
	}
	if q.err != nil {
		return q
	}
	if n < 0 {
		q.err = fmt.Errorf("offset must be >= 0")
		return q
	}
	q.offset = n
	return q
}

func (q *Query) Page(page int, pageSize int) *Query {
	if q == nil {
		return &Query{err: fmt.Errorf("query is nil"), limit: -1}
	}
	if q.err != nil {
		return q
	}
	if page < 1 {
		q.err = fmt.Errorf("page must be >= 1")
		return q
	}
	if pageSize <= 0 {
		q.err = fmt.Errorf("page size must be > 0")
		return q
	}
	q.offset = (page - 1) * pageSize
	q.limit = pageSize
	return q
}

func (q *Query) And(other *Query) *Query {
	return q.combine(other, "AND")
}

func (q *Query) Or(other *Query) *Query {
	return q.combine(other, "OR")
}

func (q *Query) Not() *Query {
	if q == nil {
		return &Query{err: fmt.Errorf("query is nil"), limit: -1}
	}
	if q.err != nil {
		return q
	}
	if q.root == nil {
		q.err = fmt.Errorf("query is empty")
		return q
	}
	q.root = &neg{inner: q.root}
	return q
}

func (q *Query) combine(other *Query, op string) *Query {
	if q == nil {
		return combineQueries(nil, other, op)
	}
	combined := combineQueries(q, other, op)
	q.root = combined.root
	q.err = combined.err
	q.limit = combined.limit
	q.offset = combined.offset
	return q
}

func combineQueries(left *Query, right *Query, op string) *Query {
	out := &Query{limit: -1}
	if left != nil && left.err != nil {
		out.err = left.err
	}
	if out.err == nil && right != nil && right.err != nil {
		out.err = right.err
	}
	if left == nil || left.root == nil {
		if right != nil {
			out.root = right.root
			copyPagination(out, right)
		}
		return out
	}
	if right == nil || right.root == nil {
		out.root = left.root
		copyPagination(out, left)
		return out
	}
	out.root = &conj{left: left.root, right: right.root, op: op}
	copyPagination(out, left)
	if !hasPagination(left) {
		copyPagination(out, right)
	}
	return out
}

func And(queries ...*Query) *Query {
	return combineMany("AND", queries...)
}

func Or(queries ...*Query) *Query {
	return combineMany("OR", queries...)
}

func Not(q *Query) *Query {
	if q == nil {
		return &Query{err: fmt.Errorf("query is nil"), limit: -1}
	}
	return q.Not()
}

func combineMany(op string, queries ...*Query) *Query {
	if len(queries) == 0 {
		return &Query{limit: -1}
	}
	result := queries[0]
	for i := 1; i < len(queries); i++ {
		result = combineQueries(result, queries[i], op)
	}
	return result
}

func (orm *ORM) Find(proto any, q *Query) ([]any, error) {
	modelInfo, err := orm.getModelInfo(proto)
	if err != nil {
		return nil, err
	}
	modelType, err := modelStructType(proto)
	if err != nil {
		return nil, err
	}
	if q != nil && q.err != nil {
		return nil, q.err
	}

	out := make([]any, 0)
	err = orm.db.View(func(tx *storage.Tx) error {
		bucketName := modelInfo.BucketName()
		modelBucket, getBucketErr := tx.GetBucket([]byte(bucketName))
		if getBucketErr == storage.ErrBucketNotFound {
			return nil
		}
		if getBucketErr != nil {
			return getBucketErr
		}

		var pks map[uint64]struct{}
		if q == nil || q.root == nil {
			pks, err = collectAllPKs(modelBucket)
		} else {
			ctx := &queryEvalContext{
				tx:          tx,
				modelInfo:   modelInfo,
				modelBucket: modelBucket,
				idxBuckets:  make(map[string]*storage.Bucket),
			}
			pks, err = orm.eval(ctx, q.root)
		}
		if err != nil {
			return err
		}

		orderedPKs := applyPagination(sortPKSet(pks), q)
		for _, pk := range orderedPKs {
			data, found := modelBucket.Get(encodeUint64BE(pk))
			if !found {
				continue
			}
			dst := reflect.New(modelType)
			if unmarshalErr := json.Unmarshal(data, dst.Interface()); unmarshalErr != nil {
				return unmarshalErr
			}
			out = append(out, dst.Interface())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func hasPagination(q *Query) bool {
	if q == nil {
		return false
	}
	return q.limit >= 0 || q.offset > 0
}

func copyPagination(dst *Query, src *Query) {
	if dst == nil || src == nil {
		return
	}
	dst.limit = src.limit
	dst.offset = src.offset
}

func applyPagination(orderedPKs []uint64, q *Query) []uint64 {
	if q == nil {
		return orderedPKs
	}

	start := q.offset
	if start < 0 {
		start = 0
	}
	if start >= len(orderedPKs) {
		return []uint64{}
	}

	end := len(orderedPKs)
	if q.limit >= 0 {
		if q.limit == 0 {
			end = start
		} else if q.limit < end-start {
			end = start + q.limit
		}
	}
	return orderedPKs[start:end]
}

type queryEvalContext struct {
	tx          *storage.Tx
	modelInfo   *ModelInfo
	modelBucket *storage.Bucket
	idxBuckets  map[string]*storage.Bucket
	allPKs      map[uint64]struct{}
}

func (ctx *queryEvalContext) getIndexBucket(indexName string) (*storage.Bucket, error) {
	if b, ok := ctx.idxBuckets[indexName]; ok {
		return b, nil
	}
	bucketName := fmt.Sprintf("index/%s/%s", ctx.modelInfo.modelName, indexName)
	bucket, err := ctx.tx.GetBucket([]byte(bucketName))
	if err == storage.ErrBucketNotFound {
		ctx.idxBuckets[indexName] = nil
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ctx.idxBuckets[indexName] = bucket
	return bucket, nil
}

func (ctx *queryEvalContext) getAllPKs() (map[uint64]struct{}, error) {
	if ctx.allPKs != nil {
		return clonePKSet(ctx.allPKs), nil
	}
	set, err := collectAllPKs(ctx.modelBucket)
	if err != nil {
		return nil, err
	}
	ctx.allPKs = set
	return clonePKSet(set), nil
}

func (orm *ORM) eval(ctx *queryEvalContext, qn queryNode) (map[uint64]struct{}, error) {
	switch node := qn.(type) {
	case *pred:
		return orm.evalPred(ctx, node)
	case *conj:
		left, err := orm.eval(ctx, node.left)
		if err != nil {
			return nil, err
		}
		right, err := orm.eval(ctx, node.right)
		if err != nil {
			return nil, err
		}
		switch node.op {
		case "AND":
			return intersectPKSets(left, right), nil
		case "OR":
			return unionPKSets(left, right), nil
		default:
			return nil, fmt.Errorf("unsupported query conjunction %q", node.op)
		}
	case *neg:
		inner, err := orm.eval(ctx, node.inner)
		if err != nil {
			return nil, err
		}
		all, err := ctx.getAllPKs()
		if err != nil {
			return nil, err
		}
		return subtractPKSets(all, inner), nil
	default:
		return nil, fmt.Errorf("unknown query node %T", qn)
	}
}

func (orm *ORM) evalPred(ctx *queryEvalContext, p *pred) (map[uint64]struct{}, error) {
	if p == nil {
		return map[uint64]struct{}{}, nil
	}
	if p.field == "" || p.op == "" {
		return nil, fmt.Errorf("invalid predicate")
	}
	fieldInfo, ok := ctx.modelInfo.LookupField(p.field)
	if !ok {
		return nil, fmt.Errorf("field %q is not indexed", p.field)
	}
	if fieldInfo.isPK {
		return evalPKPred(ctx.modelBucket, p)
	}

	encVal, err := encodeIndexValue(fieldInfo.indexType, p.val)
	if err != nil {
		return nil, err
	}
	idxBucket, err := ctx.getIndexBucket(fieldInfo.indexName)
	if err != nil {
		return nil, err
	}
	if idxBucket == nil {
		return map[uint64]struct{}{}, nil
	}

	if p.op == "=" {
		return collectPKsByIndexPrefix(idxBucket, encVal), nil
	}
	return collectPKsByIndexCompare(idxBucket, encVal, p.op)
}

func evalPKPred(bucket *storage.Bucket, p *pred) (map[uint64]struct{}, error) {
	target, err := asUint64(p.val)
	if err != nil {
		return nil, fmt.Errorf("pk query value: %w", err)
	}

	out := make(map[uint64]struct{})
	if p.op == "=" {
		if _, found := bucket.Get(encodeUint64BE(target)); found {
			out[target] = struct{}{}
		}
		return out, nil
	}

	cursor := bucket.Cursor()
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		pk, ok := decodePKKey(k)
		if !ok {
			continue
		}
		if matchUintOp(pk, target, p.op) {
			out[pk] = struct{}{}
		}
	}
	if !isSupportedOp(p.op) {
		return nil, fmt.Errorf("unsupported operator %q", p.op)
	}
	return out, nil
}

func collectAllPKs(bucket *storage.Bucket) (map[uint64]struct{}, error) {
	set := make(map[uint64]struct{})
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		pk, ok := decodePKKey(key)
		if !ok {
			continue
		}
		set[pk] = struct{}{}
	}
	return set, nil
}

func collectPKsByIndexPrefix(bucket *storage.Bucket, encVal []byte) map[uint64]struct{} {
	prefix := make([]byte, 0, len(encVal)+1)
	prefix = append(prefix, encVal...)
	prefix = append(prefix, indexPKSeparator)

	out := make(map[uint64]struct{})
	cursor := bucket.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil; key, _ = cursor.Next() {
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		_, pk, ok := splitIndexKey(key)
		if !ok {
			continue
		}
		out[pk] = struct{}{}
	}
	return out
}

func collectPKsByIndexCompare(bucket *storage.Bucket, encVal []byte, op string) (map[uint64]struct{}, error) {
	out := make(map[uint64]struct{})
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		idxVal, pk, ok := splitIndexKey(key)
		if !ok {
			continue
		}
		cmp := bytes.Compare(idxVal, encVal)
		if matchCmpOp(cmp, op) {
			out[pk] = struct{}{}
		}
	}
	if !isSupportedOp(op) {
		return nil, fmt.Errorf("unsupported operator %q", op)
	}
	return out, nil
}

func splitIndexKey(key []byte) ([]byte, uint64, bool) {
	if len(key) < 9 {
		return nil, 0, false
	}
	sepPos := len(key) - 9
	if key[sepPos] != indexPKSeparator {
		return nil, 0, false
	}
	pk := binary.BigEndian.Uint64(key[len(key)-8:])
	return key[:sepPos], pk, true
}

func decodePKKey(key []byte) (uint64, bool) {
	if len(key) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key), true
}

func asUint64(v any) (uint64, error) {
	switch n := v.(type) {
	case uint64:
		return n, nil
	case uint32:
		return uint64(n), nil
	case uint:
		return uint64(n), nil
	case int:
		if n < 0 {
			return 0, fmt.Errorf("negative integer")
		}
		return uint64(n), nil
	case int64:
		if n < 0 {
			return 0, fmt.Errorf("negative integer")
		}
		return uint64(n), nil
	case int32:
		if n < 0 {
			return 0, fmt.Errorf("negative integer")
		}
		return uint64(n), nil
	default:
		return 0, fmt.Errorf("expected unsigned integer, got %T", v)
	}
}

func matchCmpOp(cmp int, op string) bool {
	switch op {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	default:
		return false
	}
}

func matchUintOp(left uint64, right uint64, op string) bool {
	switch op {
	case "=":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func isSupportedOp(op string) bool {
	switch op {
	case "=", "!=", ">", ">=", "<", "<=":
		return true
	default:
		return false
	}
}

func unionPKSets(left, right map[uint64]struct{}) map[uint64]struct{} {
	out := clonePKSet(left)
	for pk := range right {
		out[pk] = struct{}{}
	}
	return out
}

func intersectPKSets(left, right map[uint64]struct{}) map[uint64]struct{} {
	if len(left) > len(right) {
		left, right = right, left
	}
	out := make(map[uint64]struct{}, len(left))
	for pk := range left {
		if _, ok := right[pk]; ok {
			out[pk] = struct{}{}
		}
	}
	return out
}

func subtractPKSets(base, remove map[uint64]struct{}) map[uint64]struct{} {
	out := clonePKSet(base)
	for pk := range remove {
		delete(out, pk)
	}
	return out
}

func clonePKSet(set map[uint64]struct{}) map[uint64]struct{} {
	out := make(map[uint64]struct{}, len(set))
	for pk := range set {
		out[pk] = struct{}{}
	}
	return out
}

func sortPKSet(set map[uint64]struct{}) []uint64 {
	ordered := make([]uint64, 0, len(set))
	for pk := range set {
		ordered = append(ordered, pk)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i] < ordered[j]
	})
	return ordered
}

func modelStructType(proto any) (reflect.Type, error) {
	rt := reflect.TypeOf(proto)
	if rt == nil {
		return nil, fmt.Errorf("proto is nil")
	}
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("proto must be struct or pointer to struct, got %s", rt.Kind())
	}
	return rt, nil
}
