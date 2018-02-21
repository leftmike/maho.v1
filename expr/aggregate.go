package expr

import (
	"github.com/leftmike/maho/sql"
)

type Aggregator interface {
	Accumulate(vals []sql.Value)
	Total() sql.Value
}

type MakeAggregator func() Aggregator

type countAggregator struct {
	count   int64
	nonNull bool
}

func (ca *countAggregator) Accumulate(vals []sql.Value) {
	if vals[0] != nil {
		ca.count += 1
		ca.nonNull = true
	}
}

func (ca *countAggregator) Total() sql.Value {
	if ca.nonNull {
		return sql.Int64Value(ca.count)
	}
	return nil
}

func makeCountAggregator() Aggregator {
	return &countAggregator{}
}

type countAllAggregator struct {
	count int64
}

func (caa *countAllAggregator) Accumulate(vals []sql.Value) {
	caa.count += 1
}

func (caa *countAllAggregator) Total() sql.Value {
	return sql.Int64Value(caa.count)
}

func makeCountAllAggregator() Aggregator {
	return &countAllAggregator{}
}
