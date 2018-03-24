package mvcc

import (
	"encoding/binary"

	"github.com/leftmike/maho/engine"
)

const (
	FileVersion = uint16(1)

	InventoryPageType = 1
	DirectoryPageType = 2
	TablePageType     = 3
	DataPageType      = 4
)

var (
	Signature = [16]byte{'M', 'a', 'h', 'o', 'D', 'a', 't', 'a', 'b', 'a', 's', 'e', 'F', 'i',
		'l', 'e'}
)

// summaryPage is always page 0 in the file.
type summaryPage struct {
	signature   [16]byte // 0
	fileVersion uint16   // 16
	_           uint16
	pageSize    uint32         // 20
	directory   engine.PageNum // 24
	startCount  uint32         // 32
} // 36

type SummaryPage []byte

func (sp SummaryPage) Signature() [16]byte {
	var ret [16]byte
	copy(ret[:], sp[0:16])
	return ret
}

func (sp SummaryPage) SetSignature(sig [16]byte) {
	copy(sp[0:16], sig[:])
}

func (sp SummaryPage) FileVersion() uint16 {
	return binary.LittleEndian.Uint16(sp[16:])
}

func (sp SummaryPage) SetFileVersion(u16 uint16) {
	binary.LittleEndian.PutUint16(sp[16:], u16)
}

func (sp SummaryPage) PageSize() uint32 {
	return binary.LittleEndian.Uint32(sp[20:])
}

func (sp SummaryPage) SetPageSize(u32 uint32) {
	binary.LittleEndian.PutUint32(sp[20:], u32)
}

func (sp SummaryPage) Directory() engine.PageNum {
	return engine.PageNum(binary.LittleEndian.Uint64(sp[24:]))
}

func (sp SummaryPage) SetDirectory(u64 engine.PageNum) {
	binary.LittleEndian.PutUint64(sp[24:], uint64(u64))
}

func (sp SummaryPage) StartCount() uint32 {
	return binary.LittleEndian.Uint32(sp[32:])
}

func (sp SummaryPage) SetStartCount(u32 uint32) {
	binary.LittleEndian.PutUint32(sp[32:], u32)
}

// inventoryPage keeps track of the allocated vs free status of every page in the database.
// The first inventory page is always page 1 in the file. Each page (not including the
// summaryPage) gets one bit: the bit is 0 if the page is free.
//
// Every inventory page can keep track of the allocation status of (pageSize - 1) * 8 pages.
// The database is divided into sections of (pageSize - 1) * 8 pages with the inventory page
// being the first page in each section. The first page in every section will always be
// allocated (because it is the inventory page for that section).
//
// sectionSize = (pageSize - 1) * 8
// given a page number, pn, the inventory page number, ipn, tracking that page:
// ipn = (pn - 1) / sectionSize
type inventoryPage struct {
	pageType byte   // 0
	bytes    []byte // 1
}

type InventoryPage []byte

func (ip InventoryPage) PageType() byte {
	return ip[0]
}

func (ip InventoryPage) SetPageType(u8 byte) {
	ip[0] = u8
}

func (ip InventoryPage) ByteOffset(idx int) int {
	return 1 + idx
}

func (ip InventoryPage) ByteAt(idx int) byte {
	return ip[ip.ByteOffset(idx)]
}

func (ip InventoryPage) SetByteAt(idx int, u8 byte) {
	ip[ip.ByteOffset(idx)] = u8
}

type DirectoryEntry struct {
	Table   engine.PageNum // 0
	TableID uint32         // 8
	Offset  uint16         // 12
	Length  uint16         // 14
} // 16

type directoryPage struct {
	pageType byte             // 0
	_        byte             // 1
	count    uint16           // 2
	used     uint16           // 4
	_        uint16           // 6
	next     engine.PageNum   // 8
	entries  []DirectoryEntry // 16
}

type DirectoryPage []byte

func (dp DirectoryPage) PageType() byte {
	return dp[0]
}

func (dp DirectoryPage) SetPageType(u8 byte) {
	dp[0] = u8
}

func (dp DirectoryPage) Count() uint16 {
	return binary.LittleEndian.Uint16(dp[2:])
}

func (dp DirectoryPage) SetCount(u16 uint16) {
	binary.LittleEndian.PutUint16(dp[2:], u16)
}

func (dp DirectoryPage) Used() uint16 {
	return binary.LittleEndian.Uint16(dp[4:])
}

func (dp DirectoryPage) SetUsed(u16 uint16) {
	binary.LittleEndian.PutUint16(dp[4:], u16)
}

func (dp DirectoryPage) Next() engine.PageNum {
	return engine.PageNum(binary.LittleEndian.Uint64(dp[8:]))
}

func (dp DirectoryPage) SetNext(u64 engine.PageNum) {
	binary.LittleEndian.PutUint64(dp[8:], uint64(u64))
}

func (dp DirectoryPage) EntryOffset(idx int) int {
	return 16 + idx*16
}

func (dp DirectoryPage) EntryAt(idx int) DirectoryEntry {
	off := dp.EntryOffset(idx)
	return DirectoryEntry{
		Table:   engine.PageNum(binary.LittleEndian.Uint64(dp[off:])),
		TableID: binary.LittleEndian.Uint32(dp[off+8:]),
		Offset:  binary.LittleEndian.Uint16(dp[off+12:]),
		Length:  binary.LittleEndian.Uint16(dp[off+14:]),
	}
}

func (dp DirectoryPage) SetEntryAt(idx int, de DirectoryEntry) {
	off := dp.EntryOffset(idx)
	binary.LittleEndian.PutUint64(dp[off:], uint64(de.Table))
	binary.LittleEndian.PutUint32(dp[off+8:], de.TableID)
	binary.LittleEndian.PutUint16(dp[off+12:], de.Offset)
	binary.LittleEndian.PutUint16(dp[off+14:], de.Length)
}

type tablePage struct {
	pageType  byte             // 0
	_         byte             // 1
	count     uint16           // 2
	tableID   uint32           // 4
	next      engine.PageNum   // 8
	dataPages []engine.PageNum // 12
}

type dataPage struct {
	pageType    byte     // 0
	_           [3]byte  // 1
	tableID     uint32   // 4
	sequenceNum uint32   // 8
	slotCount   uint16   // 12
	_           uint16   // 14
	bytesUsed   uint32   // 16
	_           uint32   // 20
	slots       []uint64 // 24
}
