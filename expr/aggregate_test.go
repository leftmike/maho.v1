package expr

import (
	"math"
	"testing"

	"github.com/leftmike/maho/sql"
)

func TestAggregate(t *testing.T) {
	cases := []struct {
		maker  MakeAggregator
		rows   [][]sql.Value
		result sql.Value
		fail   bool
	}{
		{
			maker: makeAvgAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{nil},
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
			},
			result: sql.Int64Value(1),
		},
		{
			maker: makeAvgAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{sql.Int64Value(2)},
			},
			result: sql.Float64Value(1.25),
		},
		{
			maker: makeAvgAggregator,
			rows: [][]sql.Value{
				{nil},
				{nil},
			},
			result: nil,
		},
		{
			maker: makeCountAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
			},
			result: sql.Int64Value(4),
		},
		{
			maker: makeCountAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{nil},
				{nil},
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
			},
			result: sql.Int64Value(4),
		},
		{
			maker: makeCountAggregator,
			rows: [][]sql.Value{
				{nil},
				{nil},
			},
			result: nil,
		},
		{
			maker: makeCountAllAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
				{nil},
				{nil},
				{sql.Int64Value(1)},
				{sql.Int64Value(1)},
			},
			result: sql.Int64Value(6),
		},
		{
			maker: makeMaxAggregator,
			rows: [][]sql.Value{
				{sql.Float64Value(100.1)},
				{sql.Int64Value(-1)},
				{sql.Int64Value(200)},
				{nil},
				{sql.Int64Value(3)},
				{sql.Int64Value(-400)},
			},
			result: sql.Int64Value(200),
		},
		{
			maker: makeMaxAggregator,
			rows: [][]sql.Value{
				{nil},
				{nil},
			},
			result: nil,
		},
		{
			maker: makeMinAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(100)},
				{sql.Float64Value(-1.1)},
				{sql.Int64Value(200)},
				{nil},
				{sql.Int64Value(3)},
				{sql.Int64Value(-400)},
			},
			result: sql.Int64Value(-400),
		},
		{
			maker: makeMinAggregator,
			rows: [][]sql.Value{
				{nil},
				{nil},
			},
			result: nil,
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(math.MaxInt64 - 5)},
				{sql.Int64Value(6)},
			},
			fail: true,
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(math.MinInt64 + 5)},
				{sql.Int64Value(-6)},
			},
			fail: true,
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(math.MaxInt64)},
				{sql.Int64Value(math.MaxInt64)},
			},
			fail: true,
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(0)},
				{sql.Int64Value(1)},
				{sql.Int64Value(2)},
				{sql.Int64Value(3)},
				{sql.Int64Value(4)},
				{sql.Int64Value(5)},
				{sql.Int64Value(6)},
				{nil},
				{sql.Int64Value(7)},
				{sql.Int64Value(8)},
				{sql.Int64Value(9)},
			},
			result: sql.Int64Value(45),
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{sql.Float64Value(1.234)},
				{sql.Int64Value(10)},
			},
			result: sql.Float64Value(11.234),
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{sql.Int64Value(10)},
				{sql.Float64Value(1.234)},
			},
			result: sql.Float64Value(11.234),
		},
		{
			maker: makeSumAggregator,
			rows: [][]sql.Value{
				{nil},
				{nil},
			},
			result: nil,
		},
	}

	for i, c := range cases {
		var failed bool
		a := c.maker()
		for j, r := range c.rows {
			err := a.Accumulate(r)
			if err != nil {
				if !c.fail {
					t.Errorf("cases[%d].Accumulate(rows[%d]) failed with %s", i, j, err)
				}
				failed = true
				break
			}
		}
		if !failed {
			tot, err := a.Total()
			if err != nil {
				if !c.fail {
					t.Errorf("cases[%d].Total() failed with %s", i, err)
				}
				failed = true
			} else if !c.fail && sql.Compare(tot, c.result) != 0 {
				t.Errorf("cases[%d].Total(): got %s want %s", i, tot, c.result)
			}
		}
		if c.fail && !failed {
			t.Errorf("cases[%d] did not fail", i)
		}
	}
}
