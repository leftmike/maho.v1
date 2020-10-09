package expr

import (
	"fmt"

	"github.com/leftmike/maho/sql"
)

type Aggregator interface {
	Accumulate(vals []sql.Value) error
	Total() (sql.Value, error)
}

type MakeAggregator func() Aggregator

type avgAggregator struct {
	sumAggregator
	count sql.Int64Value
}

func (aa *avgAggregator) Accumulate(vals []sql.Value) error {
	switch vals[0].(type) {
	case sql.Float64Value, sql.Int64Value:
		aa.count += 1
	}
	return aa.sumAggregator.Accumulate(vals)
}

func (aa *avgAggregator) Total() (sql.Value, error) {
	if aa.nonNull {
		switch s := aa.sum.(type) {
		case sql.Float64Value:
			return s / sql.Float64Value(aa.count), nil
		case sql.Int64Value:
			if s%aa.count == 0 {
				return s / aa.count, nil
			}
			return sql.Float64Value(s) / sql.Float64Value(aa.count), nil
		}
	}
	return nil, nil
}

func makeAvgAggregator() Aggregator {
	return &avgAggregator{}
}

type countAggregator struct {
	count int64
}

func (ca *countAggregator) Accumulate(vals []sql.Value) error {
	if vals[0] != nil {
		ca.count += 1
	}
	return nil
}

func (ca *countAggregator) Total() (sql.Value, error) {
	return sql.Int64Value(ca.count), nil
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

type maxAggregator struct {
	max     sql.Value
	nonNull bool
}

func (ma *maxAggregator) Accumulate(vals []sql.Value) error {
	if ma.nonNull {
		cmp, err := ma.max.Compare(vals[0])
		if err == nil && cmp < 0 {
			ma.max = vals[0]
		}
	} else {
		switch vals[0].(type) {
		case sql.Float64Value, sql.Int64Value:
			ma.max = vals[0]
			ma.nonNull = true
		}
	}
	return nil
}

func (ma *maxAggregator) Total() (sql.Value, error) {
	if ma.nonNull {
		return ma.max, nil
	}
	return nil, nil
}

func makeMaxAggregator() Aggregator {
	return &maxAggregator{}
}

type minAggregator struct {
	min     sql.Value
	nonNull bool
}

func (ma *minAggregator) Accumulate(vals []sql.Value) error {
	if ma.nonNull {
		cmp, err := ma.min.Compare(vals[0])
		if err == nil && cmp > 0 {
			ma.min = vals[0]
		}
	} else {
		switch vals[0].(type) {
		case sql.Float64Value, sql.Int64Value:
			ma.min = vals[0]
			ma.nonNull = true
		}
	}
	return nil
}

func (ma *minAggregator) Total() (sql.Value, error) {
	if ma.nonNull {
		return ma.min, nil
	}
	return nil, nil
}

func makeMinAggregator() Aggregator {
	return &minAggregator{}
}

type sumAggregator struct {
	sum     sql.Value
	nonNull bool
}

func (sa *sumAggregator) add(v2 sql.Value) error {
	switch v1 := sa.sum.(type) {
	case sql.Int64Value:
		switch v2 := v2.(type) {
		case sql.Int64Value:
			s := v1 + v2
			if (s > v1) != (v2 > 0) {
				return fmt.Errorf("engine: sum aggregator integer overflow: %d %d", v1, v2)
			}
			sa.sum = s
			return nil
		case sql.Float64Value:
			sa.sum = sql.Float64Value(v1) + v2
			return nil
		}
	case sql.Float64Value:
		switch v2 := v2.(type) {
		case sql.Int64Value:
			sa.sum = v1 + sql.Float64Value(v2)
			return nil
		case sql.Float64Value:
			sa.sum = v1 + v2
			return nil
		}
	default:
		panic(fmt.Sprintf("sql.Value must be a number: %T: %v", v1, v1))
	}
	return nil
}

func (sa *sumAggregator) Accumulate(vals []sql.Value) error {
	if sa.nonNull {
		return sa.add(vals[0])
	} else {
		switch vals[0].(type) {
		case sql.Float64Value, sql.Int64Value:
			sa.sum = vals[0]
			sa.nonNull = true
		}
	}
	return nil
}

func (sa *sumAggregator) Total() (sql.Value, error) {
	if sa.nonNull {
		return sa.sum, nil
	}
	return nil, nil
}

func makeSumAggregator() Aggregator {
	return &sumAggregator{}
}
