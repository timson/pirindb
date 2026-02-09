package orm

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timson/pirindb/storage"
)

func TestBuilderEq(t *testing.T) {
	q := Where("Age").Eq(30)

	p, ok := q.root.(*pred)
	require.True(t, ok)
	require.Equal(t, "Age", p.field)
	require.Equal(t, "=", p.op)
	require.Equal(t, 30, p.val)
}

func TestBuilderAnd(t *testing.T) {
	q := Where("Age").Gt(30).
		And(Where("City").Eq("Haifa"))

	c, ok := q.root.(*conj)
	require.True(t, ok)
	require.Equal(t, "AND", c.op)
	_, ok = c.left.(*pred)
	require.True(t, ok)
	_, ok = c.right.(*pred)
	require.True(t, ok)
}

func TestBuilderNot(t *testing.T) {
	q := Not(Where("City").Eq("Haifa"))

	n, ok := q.root.(*neg)
	require.True(t, ok)
	_, ok = n.inner.(*pred)
	require.True(t, ok)
}

func TestBuilderDoubleAnd(t *testing.T) {
	q := Where("Age").Gt(30).
		And(Where("City").Eq("Haifa")).
		And(Where("Active").Eq(true))

	root, ok := q.root.(*conj)
	require.True(t, ok)
	require.Equal(t, "AND", root.op)

	rightPred, ok := root.right.(*pred)
	require.True(t, ok)
	require.Equal(t, "Active", rightPred.field)
	require.Equal(t, "=", rightPred.op)

	leftConj, ok := root.left.(*conj)
	require.True(t, ok)
	require.Equal(t, "AND", leftConj.op)

	leftPred, ok := leftConj.left.(*pred)
	require.True(t, ok)
	require.Equal(t, "Age", leftPred.field)

	midPred, ok := leftConj.right.(*pred)
	require.True(t, ok)
	require.Equal(t, "City", midPred.field)
}

func TestFindNilQueryReturnsAll(t *testing.T) {
	o, _ := setupORMWithUsers(t)

	rows, err := o.Find(User{}, nil)
	require.NoError(t, err)
	require.Len(t, rows, 5)
}

func TestFindAndOrNot(t *testing.T) {
	o, _ := setupORMWithUsers(t)

	q := Where("Age").Gte(30).
		And(Where("City").Eq("Haifa").Or(Where("City").Eq("TelAviv"))).
		And(Not(Where("login").Eq("admin")))

	rows, err := o.Find(User{}, q)
	require.NoError(t, err)
	require.Equal(t, []string{"alice", "carol"}, userLogins(t, rows))
}

func TestFindRangeAndNe(t *testing.T) {
	o, _ := setupORMWithUsers(t)

	rows, err := o.Find(User{}, Where("Age").Gt(30).And(Where("Age").Lt(40)))
	require.NoError(t, err)
	require.Equal(t, []string{"admin", "alice", "carol"}, userLogins(t, rows))

	rows, err = o.Find(User{}, Where("City").Ne("Haifa"))
	require.NoError(t, err)
	require.Equal(t, []string{"carol", "dave"}, userLogins(t, rows))
}

func TestSaveAutoPKAndReindexOnUpdate(t *testing.T) {
	db, _ := storage.CreateTestDB(t)
	o := New(db)

	u := &User{Login: "nora", Age: 21, City: "Haifa"}
	require.NoError(t, o.Save(u))
	require.NotZero(t, u.ID)

	rows, err := o.Find(User{}, Where("City").Eq("Haifa"))
	require.NoError(t, err)
	require.Equal(t, []string{"nora"}, userLogins(t, rows))

	u.City = "Jerusalem"
	require.NoError(t, o.Save(u))

	rows, err = o.Find(User{}, Where("City").Eq("Haifa"))
	require.NoError(t, err)
	require.Empty(t, rows)

	rows, err = o.Find(User{}, Where("City").Eq("Jerusalem"))
	require.NoError(t, err)
	require.Equal(t, []string{"nora"}, userLogins(t, rows))
}

func setupORMWithUsers(t *testing.T) (*ORM, []*User) {
	db, _ := storage.CreateTestDB(t)
	o := New(db)

	users := []*User{
		{Login: "alice", Age: 31, City: "Haifa"},
		{Login: "bob", Age: 29, City: "Haifa"},
		{Login: "carol", Age: 34, City: "TelAviv"},
		{Login: "dave", Age: 41, City: "Jerusalem"},
		{Login: "admin", Age: 36, City: "Haifa"},
	}

	models := make([]any, len(users))
	for i := range users {
		models[i] = users[i]
	}
	require.NoError(t, o.Save(models...))
	for _, u := range users {
		require.NotZero(t, u.ID)
	}
	return o, users
}

func userLogins(t *testing.T, rows []any) []string {
	logins := make([]string, 0, len(rows))
	for _, row := range rows {
		u, ok := row.(*User)
		require.True(t, ok)
		logins = append(logins, u.Login)
	}
	sort.Strings(logins)
	return logins
}
