package rowcols

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
	io.Reader
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
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

func (wal *WAL) newWAL() error {
	err := wal.f.Truncate(0)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, 16)
	// walHeader.signature
	buf = append(buf, walHeaderSignature[:]...)

	// walHeader.walVersion
	buf = append(buf, walVersion)

	// walHeader.unused
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0)

	_, err = wal.f.Write(buf)
	return err
}

func (wal *WAL) ReadWAL(hndlr walHandler) error {
	fi, err := wal.f.Stat()
	if err != nil {
		return err
	}
	sz := fi.Size()
	if sz < 16 { // Need at least the header.
		return wal.newWAL()
	}

	var readBuf bytes.Buffer
	readBuf.Grow(int(sz))
	_, err = readBuf.ReadFrom(wal.f)
	if err != nil {
		return err
	}
	buf := readBuf.Bytes()

	// walHeader
	if !bytes.Equal(buf[0:8], walHeaderSignature[:]) {
		return fmt.Errorf("rowcols: bad WAL signature: %v", buf[0:8])
	}
	if buf[8] > walVersion {
		return fmt.Errorf("rowcols: bad WAL version: %d", buf[8])
	}
	buf = buf[16:]

	// XXX: read and process records

	return nil
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
	// XXX: add length to the front of the buffer

	// recordFooter.recType
	buf = append(buf, commitRecordType)
	// recordFooter.length
	buf = EncodeUint32(buf, uint32(len(buf))+4)
	_, err := wal.f.Write(buf)
	return err
}
