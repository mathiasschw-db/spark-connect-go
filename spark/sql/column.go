package sql

import (
	proto "github.com/apache/spark-connect-go/v35/internal/generated"
)

type Column struct {
}

func (c *Column) ToFilter() *proto.Filter {
	return &proto.Filter{}
}
