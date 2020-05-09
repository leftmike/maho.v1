package rowcols

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	walVersion = 1

	versionRecordType = 0
	deleteRecordType  = 1
	setRecordType     = 2
	commitRecordType  = 3
)

var (
	walHeaderSignature = [8]byte{'m', 'a', 'h', 'o', 'w', 'a', 'l', 0}
)

type walHeader struct {
	signature  [8]byte
	walVersion byte
	unused     [7]byte
}

// use routine from encode.go
func WriteUint32(w io.Writer, u32 uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u32)
	_, err := w.Write(buf)
	return err
}

// use routine from encode.go
func WriteUint64(w io.Writer, u64 uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u64)
	_, err := w.Write(buf)
	return err
}

type walFile interface {
	io.Writer
	Truncate(size int64) error
}

type WAL struct {
	f walFile
}

func MakeWAL(f walFile) (*WAL, error) {
	return &WAL{
		f: f,
	}, nil
}

type walHandler interface{}

func (wal *WAL) ReadWAL(hndlr walHandler) error {
	// XXX: read and apply WAL
	err := wal.f.Truncate(0)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	// walHeader.signature
	_, err = buf.Write(walHeaderSignature[:])
	if err != nil {
		return err
	}
	// walHeader.walVersion
	err = buf.WriteByte(walVersion)
	if err != nil {
		return err
	}
	// walHeader.unused
	_, err = buf.Write([]byte{0, 0, 0, 0, 0, 0, 0})
	if err != nil {
		return err
	}

	_, err = wal.f.Write(buf.Bytes())
	return err
}

func encodeRowItem(buf []byte, ri rowItem) []byte {
	if ri.deleted {
		buf = append(buf, deleteRecordType)
		buf = EncodeVarint(buf, ri.mid)
		buf = EncodeVarint(buf, uint64(ri.reverse))
		rowBuf := EncodeRowValue(ri.row, int(ri.numKeyCols))
		buf = EncodeVarint(buf, uint64(len(rowBuf)))
		buf = append(buf, rowBuf...)
	} else {
		buf = append(buf, setRecordType)
		buf = EncodeVarint(buf, ri.mid)
		buf = EncodeVarint(buf, uint64(ri.reverse))
		buf = append(buf, ri.numKeyCols)
		rowBuf := EncodeRowValue(ri.row, len(ri.row))
		buf = EncodeVarint(buf, uint64(len(rowBuf)))
		buf = append(buf, rowBuf...)
	}
	return buf
}

func (wal *WAL) writeCommit(buf []byte) error {
	// recordFooter.recType
	buf = append(buf, commitRecordType)
	// recordFooter.length
	buf = EncodeUint32(buf, uint32(len(buf))+4)
	_, err := wal.f.Write(buf)
	return err
}
