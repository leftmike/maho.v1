package memory

const (
	transactionFlag = 0xFFFF000000000000
	tidMask         = 0x0000000000FFFFFF
	cidMask         = tidMask
	cidShift        = 24
)

type version uint64
type tid uint32
type cid uint32

func (v version) isTransaction() bool {
	return v&transactionFlag == transactionFlag
}

func (v version) getTID() tid {
	return (tid)(v & tidMask)
}

func (v version) getCID() cid {
	return (cid)((v >> cidShift) & cidMask)
}

func makeVersion(tid tid, cid cid) version {
	return ((version)(tid)) | (((version)(cid)) << cidShift) | transactionFlag
}
