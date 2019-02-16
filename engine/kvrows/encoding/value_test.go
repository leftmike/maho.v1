package encoding_test

import (
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/leftmike/maho/engine/kvrows/encoding"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/testutil"
)

func TestEncodeVarint(t *testing.T) {
	numbers := []uint64{
		0,
		1,
		125,
		126,
		127,
		0xFF,
		0x100,
		0xFFF,
		0x1000,
		0x7F7F,
		1234567890,
		math.MaxUint32,
		math.MaxUint64,
	}

	for _, n := range numbers {
		buf := encoding.EncodeVarint(nil, n)
		pbuf := proto.EncodeVarint(n)
		if !testutil.DeepEqual(buf, pbuf) {
			t.Errorf("EncodeVarint(%d): got %v want %v", n, buf, pbuf)
		}
		ret, r, ok := encoding.DecodeVarint(buf)
		if !ok {
			t.Errorf("DecodeVarint(%v) failed", buf)
		} else if len(ret) != 0 {
			t.Errorf("DecodeVarint(%v): got %v want []", buf, ret)
		} else if n != r {
			t.Errorf("DecodeVarint(%v): got %d want %d", buf, r, n)
		}
	}
}

func TestEncodeZigzag64(t *testing.T) {
	numbers := []int64{
		0,
		1,
		125,
		126,
		127,
		128,
		129,
		0xFF,
		0x100,
		0xFFF,
		0x1000,
		0x7F7F,
		1234567890,
		10000000000,
		math.MaxInt32,
		math.MaxInt64,
		math.MinInt32,
		math.MinInt64,
		-987654321,
		-1000000000,
		-125,
		-126,
		-127,
		-128,
		-129,
		-0xFF,
	}

	for _, n := range numbers {
		buf := encoding.EncodeZigzag64(nil, n)
		enc := proto.NewBuffer(nil)
		err := enc.EncodeZigzag64(uint64(n))
		if err != nil {
			t.Errorf("proto.EncodeZigzag64(%d) failed with %s", n, err)
		} else {
			pbuf := enc.Bytes()
			if !testutil.DeepEqual(buf, pbuf) {
				t.Errorf("EncodeZigzag64(%d): got %v want %v", n, buf, pbuf)
			}
		}
		ret, r, ok := encoding.DecodeZigzag64(buf)
		if !ok {
			t.Errorf("DecodeZigzag64(%v) failed", buf)
		} else if len(ret) != 0 {
			t.Errorf("DecodeZigzag64(%v): got %v want []", buf, ret)
		} else if n != r {
			t.Errorf("DecodeZigzag64(%v): got %d want %d", buf, r, n)
		}
	}
}

func TestRowValues(t *testing.T) {
	cases := []struct {
		row []sql.Value
		s   string
	}{
		{
			row: []sql.Value{sql.BoolValue(true)},
			s:   "true",
		},
		{
			row: []sql.Value{sql.Int64Value(345)},
			s:   "345",
		},
		{
			row: []sql.Value{sql.Float64Value(987.6543)},
			s:   "987.6543",
		},
		{
			row: []sql.Value{sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
			s:   "'abcdefghijklmnopqrstuvwxyz'",
		},
		{
			row: []sql.Value{sql.BoolValue(true), sql.Int64Value(345), sql.Float64Value(987.6543),
				sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
			s: "true, 345, 987.6543, 'abcdefghijklmnopqrstuvwxyz'",
		},
		{
			row: []sql.Value{sql.BoolValue(true), nil, sql.Int64Value(345)},
			s:   "true, NULL, 345",
		},
		{
			row: []sql.Value{nil, nil, nil, sql.StringValue("ABCDEFG")},
			s:   "NULL, NULL, NULL, 'ABCDEFG'",
		},
		{
			row: []sql.Value{sql.Int64Value(1234567890), sql.StringValue(""), sql.BoolValue(true)},
			s:   "1234567890, '', true",
		},
	}

	for _, c := range cases {
		buf := encoding.MakeRowValue(c.row)
		row, ok := encoding.ParseRowValue(buf)
		if !ok {
			t.Errorf("ParseRowValue(%s) failed", c.s)
		} else if !testutil.DeepEqual(c.row, row) {
			t.Errorf("ParseRowValue(%s) got %v want %v", c.s, row, c.row)
		}

		s := encoding.FormatValue(buf)
		if s != c.s {
			t.Errorf("FormatValue: got %s want %s", s, c.s)
		}
	}
}

func testPanic(t *testing.T, n string, test func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: did not fail", n)
		}
	}()

	test()
}

func TestProtobufValues(t *testing.T) {
	testPanic(t, "MakeProtobufValue",
		func() {
			md := encoding.DatabaseMetadata{
				Type: uint32(127),
			}
			encoding.MakeProtobufValue(&md)
		})

	testPanic(t, "MakeProtobufValue",
		func() {
			md := encoding.DatabaseMetadata{}
			encoding.MakeProtobufValue(&md)
		})
	md := encoding.DatabaseMetadata{
		Type: uint32(encoding.Type_DatabaseMetadataType),
	}
	encoding.MakeProtobufValue(&md)

	testPanic(t, "MakeProtobufValue",
		func() {
			td := encoding.TableMetadata{}
			encoding.MakeProtobufValue(&td)
		})
	td := encoding.TableMetadata{
		Type: uint32(encoding.Type_TableMetadataType),
	}
	encoding.MakeProtobufValue(&td)

	cases := []struct {
		pb  proto.Message
		ret proto.Message
		s   string
	}{
		{
			pb: &encoding.DatabaseMetadata{
				Type:        uint32(encoding.Type_DatabaseMetadataType),
				Version:     555,
				Name:        "test_database",
				Opens:       10000,
				NextTableID: 5001,
				NextRowID:   12345,
			},
			ret: &encoding.DatabaseMetadata{},
			s:   `{1 555 test_database 10000 5001 12345 {} [] 0}`,
		},
		{
			pb: &encoding.TableMetadata{
				Type: uint32(encoding.Type_TableMetadataType),
				ID:   5000,
				Columns: []*encoding.ColumnMetadata{
					{
						Name:    "firstColumn",
						Index:   0,
						Type:    encoding.DataType_Character,
						Size:    6000,
						Fixed:   false,
						Binary:  true,
						NotNull: true,
						Default: "this is the default value",
					},
					{
						Name:    "second_column",
						Index:   59,
						Type:    encoding.DataType_Boolean,
						Default: "true",
					},
				},
			},
			ret: &encoding.TableMetadata{},
			s:   `{2 5000 [Name:"firstColumn" Type:Character Size:6000 Binary:true NotNull:true Default:"this is the default value"  Name:"second_column" Index:59 Type:Boolean Default:"true" ] {} [] 0}`,
		},
		{
			pb: &encoding.Transaction{
				Type:      uint32(encoding.Type_TransactionType),
				State:     uint32(encoding.TransactionState_Aborted),
				WhichOpen: 4567890,
			},
			ret: &encoding.Transaction{},
			s:   `{3 2 4567890 {} [] 0}`,
		},
		{
			pb: &encoding.Proposal{
				Type:           uint32(encoding.Type_ProposalType),
				TransactionKey: []byte("abcd"),
			},
			ret: &encoding.Proposal{},
			s:   `{4 [97 98 99 100] {} [] 0}`,
		},
	}

	for _, c := range cases {
		buf := encoding.MakeProtobufValue(c.pb)
		ok := encoding.ParseProtobufValue(buf, c.ret)
		if !ok {
			t.Error("ParseProtobufValue failed")
		} else if !testutil.DeepEqual(c.pb, c.ret) {
			t.Errorf("got %#v want %#v", c.ret, c.pb)
		}
		s := encoding.FormatValue(buf)
		if s != c.s {
			t.Errorf("FormatValue: got %s want %s", s, c.s)
		}
	}
}
