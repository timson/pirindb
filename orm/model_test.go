package orm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type User struct {
	ID    uint64 `pirin:"pk,auto"`
	Login string `pirin:"idx=login,string"`
	Age   int    `pirin:"idx=age,int"`
	City  string `pirin:"idx=city,string"`
}

func TestParseModel(t *testing.T) {
	modelInfo, err := parseModel(User{})
	require.NoError(t, err)
	require.Equal(t, "ID", modelInfo.pk.fieldName)
	require.Len(t, modelInfo.indexes, 3)
}

func TestLookupFieldSupportsAliases(t *testing.T) {
	modelInfo, err := parseModel(User{})
	require.NoError(t, err)

	_, ok := modelInfo.LookupField("City")
	require.True(t, ok)

	_, ok = modelInfo.LookupField("city")
	require.True(t, ok)

	_, ok = modelInfo.LookupField("login")
	require.True(t, ok)
}
