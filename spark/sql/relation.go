package sql

import (
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

// ensure interface type matches
var (
	_ readType = (*datasource)(nil)
	_ readType = (*namedTable)(nil)
)

type Relation interface {
	ToProtoRelation() *proto.Relation
}

type read struct {
	readType readType
}

func (r *read) ToProtoRelation() *proto.Relation {
	return &proto.Relation{
		RelType: &proto.Relation_Read{
			Read: r.readType.toRead(),
		},
	}
}

type readType interface {
	toRead() *proto.Read
}

type datasource struct {
	options []DataSourceOption
}

func (d *datasource) toRead() *proto.Read {
	source := &proto.Read_DataSource{}
	for _, option := range d.options {
		option(source)
	}
	return &proto.Read{
		ReadType: &proto.Read_DataSource_{
			DataSource: source,
		},
	}
}

type DataSourceOption func(*proto.Read_DataSource)

func Format(format string) DataSourceOption {
	return func(d *proto.Read_DataSource) {
		d.Format = &format
	}
}

func Schema(schema string) DataSourceOption {
	return func(d *proto.Read_DataSource) {
		d.Schema = &schema
	}
}

func DataSource(options ...DataSourceOption) *datasource {
	return &datasource{options: options}
}

type namedTable struct {
	name string
}

func (n namedTable) toRead() *proto.Read {
	return &proto.Read{
		ReadType: &proto.Read_NamedTable_{
			NamedTable: &proto.Read_NamedTable{
				UnparsedIdentifier: n.name,
			},
		},
	}
}

func NamedTable(name string) *namedTable {
	return &namedTable{name: name}
}

func Read(readType readType) Relation {
	return &read{
		readType: readType,
	}
}
