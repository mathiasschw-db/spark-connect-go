package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataSourceReadWith(t *testing.T) {
	format := "csv"
	readDS := Read(DataSource(Format(format)))
	proto := readDS.ToProtoRelation()
	assert.Equal(t, format, proto.GetRead().GetDataSource().GetFormat())
}

func TestDataSourceReadWithSchema(t *testing.T) {
	format := "csv"
	schema := "schema"
	readDS := Read(DataSource(Format(format), Schema(schema)))
	proto := readDS.ToProtoRelation()
	assert.Equal(t, format, proto.GetRead().GetDataSource().GetFormat())
	assert.Equal(t, schema, proto.GetRead().GetDataSource().GetSchema())
}

func TestNamedTableRead(t *testing.T) {
	name := "table"
	readNT := Read(NamedTable(name))
	proto := readNT.ToProtoRelation()
	assert.Equal(t, name, proto.GetRead().GetNamedTable().GetUnparsedIdentifier())
}
