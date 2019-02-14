package encoding_test

import (
	"math"
	"testing"

	"github.com/leftmike/maho/engine/kvrows/encoding"
)

func TestVersion(t *testing.T) {
	vers := []encoding.Version{
		encoding.MakeTransactionVersion(1),
		encoding.MakeTransactionVersion(2),
		encoding.MakeTransactionVersion(999),
		encoding.MakeTransactionVersion(100000),
		encoding.MakeTransactionVersion(math.MaxUint32),
		encoding.Version(math.MaxUint32 + 1),
		encoding.Version(12345678901234567890),
		encoding.Version(12345678901234567891),
		encoding.Version(math.MaxUint64 - math.MaxUint32 - 2),
		encoding.MakeProposedWriteVersion(0),
		encoding.MakeProposedWriteVersion(1),
		encoding.MakeProposedWriteVersion(2),
		encoding.MakeProposedWriteVersion(100),
		encoding.MakeProposedWriteVersion(99999),
		encoding.MakeProposedWriteVersion(math.MaxUint32),
		encoding.ProposalVersion,
	}

	prevVer := encoding.Version(0)
	for _, ver := range vers {
		if ver <= prevVer {
			t.Errorf("ver %d not greater than previous ver %d", ver, prevVer)
		}
		prevVer = ver
	}

	cases := []struct {
		ver        encoding.Version
		isProposal bool
		isWrite    bool
		isTx       bool
		u          uint32
	}{
		{ver: encoding.ProposalVersion, isProposal: true},
		{ver: encoding.MakeProposedWriteVersion(1234), isWrite: true, u: 1234},
		{ver: encoding.Version(12345678901234567890)},
		{ver: encoding.MakeTransactionVersion(56789), isTx: true, u: 56789},
	}

	for _, c := range cases {
		ret := encoding.IsProposal(c.ver)
		if ret != c.isProposal {
			t.Errorf("encoding.IsProposal(%d) got %v want %v", c.ver, ret, c.isProposal)
		}
		ret = encoding.IsProposedWrite(c.ver)
		if ret != c.isWrite {
			t.Errorf("encoding.IsProposedWrite(%d) got %v want %v", c.ver, ret, c.isWrite)
		}
		if c.isWrite {
			u := encoding.ProposedWriteStmtID(c.ver)
			if u != c.u {
				t.Errorf("encoding.ProposedWriteStmtID(%d) got %d want %d", c.ver, u, c.u)
			}
		}
		ret = encoding.IsTransaction(c.ver)
		if ret != c.isTx {
			t.Errorf("encoding.IsTransaction(%d) got %v want %v", c.ver, ret, c.isTx)
		}
		if c.isTx {
			u := encoding.TransactionID(c.ver)
			if u != c.u {
				t.Errorf("encoding.TransactionID(%d) got %d want %d", c.ver, u, c.u)
			}
		}
	}
}
