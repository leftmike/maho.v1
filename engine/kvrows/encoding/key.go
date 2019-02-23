package encoding

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/leftmike/maho/sql"
)

// Keys look like <table-id><index-id>[<sql-value>...][<suffix>]<type>, where
// <table-id> and <index-id> are uint32s, <sql-value> are zero or more SQL
// values encoded to sort properly as binary strings, <type> is a single byte
// which specifies the type of key, <suffix> is optional depending upon the
// type.

const (
	// The SQL values are encoded as a tag followed by a binary representation
	// of the value.
	NullKeyTag        = 128
	BoolKeyTag        = 129
	Int64NegKeyTag    = 130
	Int64NotNegKeyTag = 131
	Float64NaNKeyTag  = 140
	Float64NegKeyTag  = 141
	Float64ZeroKeyTag = 142
	Float64PosKeyTag  = 143
	StringKeyTag      = 150

	// Using these key types and suffixes means that for a given bare key, the
	// keys will be ordered as proposal, proposed writes, and finally versions.
	// The statement ids and versions are encoded so they sort in descending
	// order; ie. higher statement ids and versions will come before lower ones.
	//
	// Next key type and next bare key type are used in ranges. They will never
	// be stored.
	NextKeyType          = KeyType(0)   // ... [ <suffix> ] <key-type>
	BareKeyType          = KeyType(1)   // No suffix
	ProposalKeyType      = KeyType(2)   // No suffix
	ProposedWriteKeyType = KeyType(3)   // Suffix: 2 <statement-id>
	VersionKeyType       = KeyType(4)   // Suffix: 3 <version>
	TransactionKeyType   = KeyType(5)   // Suffix: <transaction-id>
	NextBareKeyType      = KeyType(255) // No suffix
)

type KeyType byte
type Version uint64

func makeKey(tid, iid uint32, vals []sql.Value) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key, tid)
	binary.BigEndian.PutUint32(key[4:], iid)

	for _, val := range vals {
		switch val := val.(type) {
		case sql.BoolValue:

			key = append(key, BoolKeyTag)
			if val {
				key = append(key, 1)
			} else {
				key = append(key, 0)
			}
		case sql.StringValue:
			key = append(key, StringKeyTag)
			key = encodeKeyBytes(key, []byte(val))
		case sql.Float64Value:
			if math.IsNaN(float64(val)) {
				key = append(key, Float64NaNKeyTag)
			} else if val == 0 {
				key = append(key, Float64ZeroKeyTag)
			} else {
				u := math.Float64bits(float64(val))
				if u&(1<<63) != 0 {
					u = ^u
					key = append(key, Float64NegKeyTag)
				} else {
					key = append(key, Float64PosKeyTag)
				}
				key = encodeUInt64(key, u)
			}
		case sql.Int64Value:
			if val < 0 {
				key = append(key, Int64NegKeyTag)
			} else {
				key = append(key, Int64NotNegKeyTag)
			}
			key = encodeUInt64(key, uint64(val))
		default:
			if val == nil {
				key = append(key, NullKeyTag)
			} else {
				panic(fmt.Sprintf("unexpected type for sql.Value: %T: %v", val, val))
			}
		}
	}
	return key
}

func MakeBareKey(tid, iid uint32, vals []sql.Value) []byte {
	return append(makeKey(tid, iid, vals), byte(BareKeyType))
}

func MakeProposalKey(tid, iid uint32, vals []sql.Value) []byte {
	return append(makeKey(tid, iid, vals), byte(ProposalKeyType))
}

func MakeProposedWriteKey(tid, iid uint32, vals []sql.Value, stmtid uint32) []byte {
	key := makeKey(tid, iid, vals)
	key = append(key, byte(ProposedWriteKeyType))
	key = encodeUInt32(key, ^stmtid) // Sort in descending order.
	return append(key, byte(ProposedWriteKeyType))
}

func MakeTransactionKey(tid, iid uint32, vals []sql.Value, txid uint32) []byte {
	key := makeKey(tid, iid, vals)
	key = encodeUInt32(key, txid)
	return append(key, byte(TransactionKeyType))
}

func MakeVersionKey(tid, iid uint32, vals []sql.Value, ver Version) []byte {
	key := makeKey(tid, iid, vals)
	key = append(key, byte(VersionKeyType))
	key = encodeUInt64(key, ^uint64(ver)) // Sort in descending order.
	return append(key, byte(VersionKeyType))
}

// MakeNextKey will return a key which can be used to start a key scan
// immediately following the key argument.
func MakeNextKey(key []byte) []byte {
	return append(key, byte(NextKeyType))
}

// MakeNextBareKey will return a key which can be used to start a key scan
// with the next bare key; it will sort after any other keys (ie. proposal
// and proposed writes) with the same prefix.
func MakeNextBareKey(key []byte) []byte {
	if KeyType(key[len(key)-1]) != BareKeyType {
		panic(fmt.Sprintf("not a bare key: %v", key))
	}
	nxt := append([]byte(nil), key...)
	nxt[len(key)-1] = byte(NextBareKeyType)
	return nxt
}

func encodeUInt64(buf []byte, u uint64) []byte {
	// Use binary.BigEndian.Uint64 to decode.
	return append(buf, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func encodeUInt32(buf []byte, u uint32) []byte {
	// Use binary.BigEndian.Uint32 to decode.
	return append(buf, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func encodeKeyBytes(buf []byte, bytes []byte) []byte {
	for _, b := range bytes {
		if b == 0 || b == 1 {
			buf = append(buf, 1)
		}
		buf = append(buf, b)
	}
	return append(buf, 0)
}

func decodeKeyBytes(key []byte) ([]byte, []byte, bool) {
	var bytes []byte
	var esc bool
	for idx, b := range key {
		if esc {
			bytes = append(bytes, b)
			esc = false
		} else if b == 0 {
			return key[idx+1:], bytes, true
		} else if b == 1 {
			esc = true
		} else {
			bytes = append(bytes, b)
		}
	}
	return nil, nil, false
}

func parseSuffix(key []byte) (KeyType, []byte, bool) {
	kt := KeyType(key[len(key)-1])
	switch kt {
	case NextKeyType:
		if len(key) < 1 {
			return 0, nil, false
		}
		return parseSuffix(key[:len(key)-1])
	case BareKeyType:
		if len(key) < 1 {
			return 0, nil, false
		}
		key = key[:len(key)-1]
	case ProposalKeyType:
		if len(key) < 1 {
			return 0, nil, false
		}
		key = key[:len(key)-1]
	case ProposedWriteKeyType:
		if len(key) < 6 || KeyType(key[len(key)-6]) != ProposedWriteKeyType {
			return 0, nil, false
		}
		// 2 uint32 2
		key = key[:len(key)-6]
	case VersionKeyType:
		if len(key) < 10 || KeyType(key[len(key)-10]) != VersionKeyType {
			return 0, nil, false
		}
		// 3 uint64 3
		key = key[:len(key)-10]
	case TransactionKeyType:
		if len(key) < 5 {
			return 0, nil, false
		}
		// uint32 4
		key = key[:len(key)-5]
	case NextBareKeyType:
		if len(key) < 1 {
			return 0, nil, false
		}
		key = key[:len(key)-1]
	default:
		return 0, nil, false
	}
	return kt, key, true
}

func ParseKey(key []byte) (uint32, uint32, []sql.Value, KeyType, bool) {
	if len(key) < 9 {
		return 0, 0, nil, 0, false
	}
	tid := binary.BigEndian.Uint32(key)
	iid := binary.BigEndian.Uint32(key[4:])

	kt, key, ok := parseSuffix(key[8:])
	if !ok {
		return 0, 0, nil, 0, false
	}

	var vals []sql.Value
	for len(key) > 0 {
		switch key[0] {
		case NullKeyTag:
			vals = append(vals, nil)
			key = key[1:]
		case BoolKeyTag:
			if len(key) < 1 {
				return 0, 0, nil, 0, false
			}
			if key[1] == 0 {
				vals = append(vals, sql.BoolValue(false))
			} else {
				vals = append(vals, sql.BoolValue(true))
			}
			key = key[2:]
		case StringKeyTag:
			var s []byte
			var ok bool
			key, s, ok = decodeKeyBytes(key[1:])
			if !ok {
				return 0, 0, nil, 0, false
			}
			vals = append(vals, sql.StringValue(s))
		case Float64NaNKeyTag:
			vals = append(vals, sql.Float64Value(math.NaN()))
			key = key[1:]
		case Float64NegKeyTag:
			var u uint64
			if len(key) < 9 {
				return 0, 0, nil, 0, false
			}
			u = ^binary.BigEndian.Uint64(key[1:])
			vals = append(vals, sql.Float64Value(math.Float64frombits(u)))
			key = key[9:]
		case Float64ZeroKeyTag:
			vals = append(vals, sql.Float64Value(0.0))
			key = key[1:]
		case Float64PosKeyTag:
			var u uint64
			if len(key) < 9 {
				return 0, 0, nil, 0, false
			}
			u = binary.BigEndian.Uint64(key[1:])
			vals = append(vals, sql.Float64Value(math.Float64frombits(u)))
			key = key[9:]
		case Int64NegKeyTag, Int64NotNegKeyTag:
			var u uint64
			if len(key) < 9 {
				return 0, 0, nil, 0, false
			}
			u = binary.BigEndian.Uint64(key[1:])
			vals = append(vals, sql.Int64Value(u))
			key = key[9:]
		default:
			return 0, 0, nil, 0, false
		}
	}

	return tid, iid, vals, kt, true
}

func GetKeyType(key []byte) KeyType {
	return KeyType(key[len(key)-1])
}

func GetKeyPrefix(key []byte) []byte {
	if KeyType(key[len(key)-1]) != BareKeyType {
		panic(fmt.Sprintf("not a bare key: %v", key))
	}
	return key[:len(key)-1]
}

func GetKeyVersion(key []byte) Version {
	if len(key) < 18 || KeyType(key[len(key)-1]) != VersionKeyType ||
		KeyType(key[len(key)-10]) != VersionKeyType {

		panic(fmt.Sprintf("not a version key: %v", key))
	}
	return Version(^binary.BigEndian.Uint64(key[len(key)-9 : len(key)-1]))
}

func GetKeyStatementID(key []byte) uint32 {
	if len(key) < 14 || KeyType(key[len(key)-1]) != ProposedWriteKeyType ||
		KeyType(key[len(key)-6]) != ProposedWriteKeyType {

		panic(fmt.Sprintf("not a proposed write key: %v", key))
	}
	return ^binary.BigEndian.Uint32(key[len(key)-5 : len(key)-1])
}

func GetKeyTransactionID(key []byte) uint32 {
	if len(key) < 13 || KeyType(key[len(key)-1]) != TransactionKeyType {
		panic(fmt.Sprintf("not a transaction key: %v", key))
	}
	return binary.BigEndian.Uint32(key[len(key)-5 : len(key)-1])
}

func FormatKey(key []byte) string {
	tid, iid, vals, kt, ok := ParseKey(key)
	if !ok {
		return fmt.Sprintf("bad key: %v", key)
	}

	s := fmt.Sprintf("/%d/%d", tid, iid)
	for _, val := range vals {
		if sv, ok := val.(sql.StringValue); ok {
			s = fmt.Sprintf("%s/%s", s, string(sv))
		} else {
			s = fmt.Sprintf("%s/%s", s, sql.Format(val))
		}
	}

	switch kt {
	case ProposalKeyType:
		s = fmt.Sprintf("%s@proposal", s)
	case ProposedWriteKeyType:
		s = fmt.Sprintf("%s@stmt(%d)", s, GetKeyStatementID(key))
	case VersionKeyType:
		s = fmt.Sprintf("%s@%d", s, GetKeyVersion(key))
	case TransactionKeyType:
		s = fmt.Sprintf("%s@txid(%d)", s, GetKeyTransactionID(key))
	case NextBareKeyType:
		s = fmt.Sprintf("%s@next-bare", s)
	}
	return s
}
