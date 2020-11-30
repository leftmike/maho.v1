package expr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/util"
)

const (
	boolLiteralTag    = 1
	int64LiteralTag   = 2
	float64LiteralTag = 3
	stringLiteralTag  = 4
	bytesLiteralTag   = 5
	colRefTag         = 6
	callTag           = 7
)

func Encode(ce sql.CExpr) []byte {
	if ce == nil {
		return nil
	}
	return encode(nil, ce)
}

func encodeString(s string, buf []byte) []byte {
	b := []byte(s)
	buf = util.EncodeVarint(buf, uint64(len(b)))
	buf = append(buf, b...)
	return buf
}

func encode(buf []byte, ce sql.CExpr) []byte {
	switch ce := ce.(type) {
	case *Literal:
		switch val := ce.Value.(type) {
		case sql.BoolValue:
			buf = append(buf, boolLiteralTag)
			if val {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case sql.StringValue:
			b := []byte(val)
			buf = append(buf, stringLiteralTag)
			buf = util.EncodeVarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		case sql.BytesValue:
			b := []byte(val)
			buf = append(buf, bytesLiteralTag)
			buf = util.EncodeVarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		case sql.Float64Value:
			buf = append(buf, float64LiteralTag)
			buf = util.EncodeUint64(buf, math.Float64bits(float64(val)))
		case sql.Int64Value:
			buf = append(buf, int64LiteralTag)
			buf = util.EncodeZigzag64(buf, int64(val))
		default:
			panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", ce, ce))
		}
	case *colRef:
		buf = append(buf, colRefTag)
		buf = util.EncodeZigzag64(buf, int64(ce.idx))
		buf = util.EncodeZigzag64(buf, int64(ce.nest))
		buf = util.EncodeVarint(buf, uint64(len(ce.ref)))
		for _, r := range ce.ref {
			buf = encodeString(r.String(), buf)
		}
	case *rowsExpr:
		panic("engine: statement expressions may not be encoded")
	case *call:
		buf = append(buf, callTag)
		buf = encodeString(ce.call.name, buf)
		buf = util.EncodeVarint(buf, uint64(len(ce.args)))
		for _, a := range ce.args {
			buf = encode(buf, a)
		}
	case param:
		panic("engine: parameters may not be encoded")
	default:
		panic(fmt.Sprintf("unexpected type for sql.CExpr: %T: %v", ce, ce))
	}

	return buf
}

func Decode(buf []byte) (sql.CExpr, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	ce, b := decode(buf)
	if ce == nil || len(b) != 0 {
		return nil, fmt.Errorf("engine: unable to decode compiled expression: %v", buf)
	}
	return ce, nil
}

func decodeString(buf []byte) ([]byte, string, bool) {
	var ok bool
	var u uint64

	buf, u, ok = util.DecodeVarint(buf)
	if !ok {
		return nil, "", false
	}
	if len(buf) < int(u) {
		return nil, "", false
	}

	s := string(buf[:u])
	buf = buf[u:]
	return buf, s, true
}

func decode(buf []byte) (sql.CExpr, []byte) {
	if len(buf) == 0 {
		return nil, nil
	}

	tag := buf[0]
	buf = buf[1:]
	switch tag {
	case boolLiteralTag:
		if len(buf) < 1 {
			return nil, nil
		}
		var val sql.Value
		if buf[0] == 0 {
			val = sql.BoolValue(false)
		} else {
			val = sql.BoolValue(true)
		}
		buf = buf[1:]
		return &Literal{val}, buf
	case stringLiteralTag:
		var ok bool
		var u uint64
		buf, u, ok = util.DecodeVarint(buf)
		if !ok {
			return nil, nil
		}
		if len(buf) < int(u) {
			return nil, nil
		}
		val := sql.StringValue(buf[:u])
		buf = buf[u:]
		return &Literal{val}, buf
	case bytesLiteralTag:
		var ok bool
		var u uint64
		buf, u, ok = util.DecodeVarint(buf)
		if !ok {
			return nil, nil
		}
		if len(buf) < int(u) {
			return nil, nil
		}
		val := sql.BytesValue(buf[:u])
		buf = buf[u:]
		return &Literal{val}, buf
	case float64LiteralTag:
		if len(buf) < 8 {
			return nil, nil
		}
		u := binary.BigEndian.Uint64(buf)
		val := sql.Float64Value(math.Float64frombits(u))
		buf = buf[8:]
		return &Literal{val}, buf
	case int64LiteralTag:
		var n int64
		var ok bool
		buf, n, ok = util.DecodeZigzag64(buf)
		if !ok {
			return nil, nil
		}
		return &Literal{sql.Int64Value(n)}, buf
	case colRefTag:
		var idx, nest int64
		var u uint64
		var ok bool

		buf, idx, ok = util.DecodeZigzag64(buf)
		if !ok {
			return nil, nil
		}
		buf, nest, ok = util.DecodeZigzag64(buf)

		buf, u, ok = util.DecodeVarint(buf)
		if !ok {
			return nil, nil
		}
		cr := &colRef{
			idx:  int(idx),
			nest: int(nest),
		}
		for u > 0 {
			u -= 1

			var s string
			buf, s, ok = decodeString(buf)
			if !ok {
				return nil, nil
			}
			cr.ref = append(cr.ref, sql.QuotedID(s))
		}
		return cr, buf
	case callTag:
		var ok bool
		var u uint64
		var nam string

		buf, nam, ok = decodeString(buf)
		if !ok {
			return nil, nil
		}
		cf, ok := funcs[nam]
		if !ok {
			return nil, nil
		}

		buf, u, ok = util.DecodeVarint(buf)
		if !ok {
			return nil, nil
		}
		c := &call{
			call: cf,
			args: make([]sql.CExpr, u),
		}
		for i := range c.args {
			c.args[i], buf = decode(buf)
			if c.args[i] == nil {
				return nil, nil
			}
		}
		return c, buf
	default:
		return nil, nil
	}
}

func (l *Literal) Eval(ctx context.Context, tx sql.Transaction, ectx sql.EvalContext) (sql.Value,
	error) {

	return l.Value, nil
}

type colRef struct {
	idx  int
	nest int
	ref  Ref
}

func (cr *colRef) String() string {
	if cr.ref == nil {
		return fmt.Sprintf("[%d.%d]", cr.nest, cr.idx)
	}
	return cr.ref.String()
}

func (cr *colRef) Eval(ctx context.Context, tx sql.Transaction, ectx sql.EvalContext) (sql.Value,
	error) {

	return ectx.EvalRef(cr.idx, cr.nest), nil
}

func ColumnIndex(ce sql.CExpr) (int, bool) {
	if cr, ok := ce.(*colRef); ok && cr.nest == 0 {
		return cr.idx, true
	}
	return 0, false
}

type rowsExpr struct {
	rowsPlan evaluate.RowsPlan
}

func (_ rowsExpr) String() string {
	return "rows plan"
}

func (re rowsExpr) Eval(ctx context.Context, tx sql.Transaction, ectx sql.EvalContext) (sql.Value,
	error) {

	rows, err := re.rowsPlan.Rows(ctx, tx, ectx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.NumColumns() != 1 {
		return nil, errors.New("engine: expected one column for scalar subquery")
	}
	dest := []sql.Value{nil}
	err = rows.Next(ctx, dest)
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	err = rows.Next(ctx, []sql.Value{nil})
	if err == nil {
		return nil, errors.New("engine: expected one row for scalar subquery")
	} else if err != io.EOF {
		return nil, err
	}
	return dest[0], nil
}

type call struct {
	call *callFunc
	args []sql.CExpr
}

func (c *call) String() string {
	s := fmt.Sprintf("%s(", c.call.name)
	for i, a := range c.args {
		if i > 0 {
			s += ", "
		}
		s += a.String()
	}
	s += ")"
	return s
}

func (c *call) Eval(ctx context.Context, tx sql.Transaction, ectx sql.EvalContext) (sql.Value,
	error) {

	args := make([]sql.Value, len(c.args))
	for i, a := range c.args {
		var err error
		args[i], err = a.Eval(ctx, tx, ectx)
		if err != nil {
			return nil, err
		} else if args[i] == nil && !c.call.handleNull {
			return nil, nil
		}
	}
	return c.call.fn(ectx, args)
}

type param struct {
	num int
	ptr *sql.Value
}

func (p param) String() string {
	return fmt.Sprintf("$%d = %s", p.num, *p.ptr)
}

func (p param) Eval(ctx context.Context, tx sql.Transaction, ectx sql.EvalContext) (sql.Value,
	error) {

	return *p.ptr, nil
}

func numFunc(a0 sql.Value, a1 sql.Value, ifn func(i0, i1 sql.Int64Value) sql.Value,
	ffn func(f0, f1 sql.Float64Value) sql.Value) (sql.Value, error) {

	switch a0 := a0.(type) {
	case sql.Float64Value:
		switch a1 := a1.(type) {
		case sql.Float64Value:
			return ffn(a0, a1), nil
		case sql.Int64Value:
			return ffn(a0, sql.Float64Value(a1)), nil
		}
	case sql.Int64Value:
		switch a1 := a1.(type) {
		case sql.Float64Value:
			return ffn(sql.Float64Value(a0), a1), nil
		case sql.Int64Value:
			return ifn(a0, a1), nil
		}
	default:
		return nil, fmt.Errorf("engine: want number got %v", a0)
	}
	return nil, fmt.Errorf("engine: want number got %v", a1)
}

func numType(args []sql.ColumnType) sql.ColumnType {
	for adx := range args {
		if args[adx].Type == sql.FloatType {
			return sql.ColumnType{Type: sql.FloatType, Size: 8}
		}
	}
	return sql.ColumnType{Type: sql.IntegerType, Size: 8}
}

func intFunc(a0 sql.Value, a1 sql.Value, ifn func(i0, i1 sql.Int64Value) sql.Value) (sql.Value,
	error) {

	if a0, ok := a0.(sql.Int64Value); ok {
		if a1, ok := a1.(sql.Int64Value); ok {
			return ifn(a0, a1), nil
		}
		return nil, fmt.Errorf("engine: want integer got %v", a1)
	}
	return nil, fmt.Errorf("engine: want integer got %v", a0)
}

func shiftFunc(a0 sql.Value, a1 sql.Value,
	ifn func(i0 sql.Int64Value, i1 uint64) sql.Value) (sql.Value, error) {

	if a0, ok := a0.(sql.Int64Value); ok {
		if a1, ok := a1.(sql.Int64Value); ok {
			if a1 < 0 {
				return nil, fmt.Errorf("engine: want non-negative integer got %v", a1)
			}
			return ifn(a0, uint64(a1)), nil
		}
		return nil, fmt.Errorf("engine: want integer got %v", a1)
	}
	return nil, fmt.Errorf("engine: want integer got %v", a0)
}

func addCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 + i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 + f1
		})
}

func andCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	if a0, ok := args[0].(sql.BoolValue); ok {
		if a1, ok := args[1].(sql.BoolValue); ok {
			return a0 && a1, nil
		}
		return nil, fmt.Errorf("engine: want boolean got %v", args[1])
	}
	return nil, fmt.Errorf("engine: want boolean got %v", args[0])
}

func binaryAndCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 & i1
		})
}

func binaryOrCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 | i1
		})
}

func concatCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	s := ""
	for _, a := range args {
		if a == nil {
			continue
		}
		switch v := a.(type) {
		case sql.BoolValue:
			if v {
				s += sql.TrueString
			} else {
				s += sql.FalseString
			}
		case sql.StringValue:
			s += string(v)
		case sql.BytesValue:
			s += fmt.Sprintf("%v", v)
		case sql.Float64Value:
			s += fmt.Sprintf("%v", v)
		case sql.Int64Value:
			s += fmt.Sprintf("%v", v)
		default:
			panic("unexpected sql.Value")
		}
	}
	return sql.StringValue(s), nil
}

func divideCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 / i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 / f1
		})
}

func equalCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp == 0), nil
}

func greaterEqualCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp >= 0), nil
}

func greaterThanCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp > 0), nil
}

func lessEqualCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp <= 0), nil
}

func lessThanCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp < 0), nil
}

func lShiftCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return shiftFunc(args[0], args[1],
		func(i0 sql.Int64Value, i1 uint64) sql.Value {
			return i0 << i1
		})
}

func moduloCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return intFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 % i1
		})
}

func multiplyCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 * i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 * f1
		})
}

func negateCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	switch a0 := args[0].(type) {
	case sql.Float64Value:
		return -a0, nil
	case sql.Int64Value:
		return -a0, nil
	}
	return nil, fmt.Errorf("engine: want number got %v", args[0])
}

func notEqualCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	cmp, err := args[0].Compare(args[1])
	if err != nil {
		return nil, err
	}
	return sql.BoolValue(cmp != 0), nil
}

func notCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	if a0, ok := args[0].(sql.BoolValue); ok {
		return sql.BoolValue(a0 == false), nil
	}
	return nil, fmt.Errorf("engine: want boolean got %v", args[0])
}

func orCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	if a0, ok := args[0].(sql.BoolValue); ok {
		if a1, ok := args[1].(sql.BoolValue); ok {
			return a0 || a1, nil
		}
	}
	return nil, fmt.Errorf("engine: want boolean got %v", args[0])
}

func rShiftCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return shiftFunc(args[0], args[1],
		func(i0 sql.Int64Value, i1 uint64) sql.Value {
			return i0 >> i1
		})
}

func subtractCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return numFunc(args[0], args[1],
		func(i0, i1 sql.Int64Value) sql.Value {
			return i0 - i1
		},
		func(f0, f1 sql.Float64Value) sql.Value {
			return f0 - f1
		})
}

func absCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	switch a0 := args[0].(type) {
	case sql.Float64Value:
		if a0 < 0 {
			return -a0, nil
		}
		return a0, nil
	case sql.Int64Value:
		if a0 < 0 {
			return -a0, nil
		}
		return a0, nil
	}
	return nil, fmt.Errorf("engine: want number got %v", args[0])
}

var (
	rowID = uint64(0)
)

func uniqueRowIDCall(ctx sql.EvalContext, args []sql.Value) (sql.Value, error) {
	return sql.Int64Value(atomic.AddUint64(&rowID, 1)), nil
}
