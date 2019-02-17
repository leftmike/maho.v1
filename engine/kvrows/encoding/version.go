package encoding

import (
	"math"
)

const (
	ProposalVersion         = Version(math.MaxUint64)
	MinVersionedTID         = 4096
	maxTransactionVersion   = math.MaxUint32
	minProposedWriteVersion = Version(math.MaxUint64 - math.MaxUint32 - 1)
	MinVersion              = maxTransactionVersion + 1
)

type Version uint64

func IsProposal(ver Version) bool {
	return ver == ProposalVersion
}

func MakeProposedWriteVersion(sid uint32) Version {
	return minProposedWriteVersion + Version(sid)
}

func IsProposedWrite(ver Version) bool {
	return ver < ProposalVersion && ver >= minProposedWriteVersion
}

func ProposedWriteStmtID(ver Version) uint32 {
	return uint32(ver - minProposedWriteVersion)
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
