package expr

import (
	"github.com/leftmike/maho/sql"
)

type Aggregator interface {
	Accumulate(vals []sql.Value) error
	Total() (sql.Value, error)
}

type MakeAggregator func() Aggregator

type countAggregator struct {
	count   int64
	nonNull bool
}

func (ca *countAggregator) Accumulate(vals []sql.Value) error {
	if vals[0] != nil {
		ca.count += 1
		ca.nonNull = true
	}
	return nil
}

func (ca *countAggregator) Total() (sql.Value, error) {
	if ca.nonNull {
		return sql.Int64Value(ca.count), nil
	}
	return nil, nil
}

func makeCountAggregator() Aggregator {
	return &countAggregator{}
}

type countAllAggregator struct {
	count int64
}

func (caa *countAllAggregator) Accumulate(vals []sql.Value) error {
	caa.count += 1
	return nil
}

func (caa *countAllAggregator) Total() (sql.Value, error) {
	return sql.Int64Value(caa.count), nil
}

func makeCountAllAggregator() Aggregator {
	return &countAllAggregator{}
}

// AVG, MAX, MIN, SUM, ANY, EVERY
// GROUP_CONCAT
