package encoding

import (
	"math"
)

const (
	ProposalVersion         = Version(math.MaxUint64)
	MinVersionedTID         = 1000
	maxTransactionVersion   = math.MaxUint32
	maxProposedWriteVersion = Version(math.MaxUint64 - 1)
	minProposedWriteVersion = Version(math.MaxUint64 - math.MaxUint32)
)

type Version uint64

func IsProposal(ver Version) bool {
	return ver == ProposalVersion
}

func MakeProposedWriteVersion(sid uint32) Version {
	return maxProposedWriteVersion - Version(sid)
}

func IsProposedWrite(ver Version) bool {
	return ver <= maxProposedWriteVersion && ver >= minProposedWriteVersion
}

func ProposedWriteStmtID(ver Version) uint32 {
	return uint32(maxProposedWriteVersion - ver)
}

func MakeTransactionVersion(txid uint32) Version {
	return Version(txid)
}

func IsTransaction(ver Version) bool {
	return ver <= maxTransactionVersion
}

func TransactionID(ver Version) uint32 {
	return uint32(ver)
}
