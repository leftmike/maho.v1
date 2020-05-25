package rowcols

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/leftmike/maho/engine/encode"
)

const (
	walVersion = 1

	commitRecordType = 1
	deleteRecordType = 2
	setRecordType    = 3
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

type walHandler interface {
	RowItem(ri rowItem) error
}

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

func decodeCommit(hndlr walHandler, ver uint64, buf []byte) error {
	for len(buf) > 0 {
		var ok bool
		var mid, reverse uint64

		typ := buf[0]
		buf = buf[1:]
		if typ != deleteRecordType && typ != setRecordType {
			return fmt.Errorf("rowcols: bad WAL record type, got %d", typ)
		}

		buf, mid, ok = encode.DecodeVarint(buf)
		if !ok {
			return errors.New("rowcols: bad WAL record, mid field")
		}
		buf, reverse, ok = encode.DecodeVarint(buf)
		if !ok || reverse > math.MaxUint32 {
			return errors.New("rowcols: bad WAL record, reverse field")
		}

		if len(buf) == 0 {
			return errors.New("rowcols: truncated WAL record")
		}
		nkc := buf[0]
		buf = buf[1:]

		var rbl uint64
		buf, rbl, ok = encode.DecodeVarint(buf)
		if !ok {
			return errors.New("rowcols: bad WAL record, row length field")
		}
		if len(buf) < int(rbl) {
			return fmt.Errorf("rowcols: bad WAL record length, have %d, want %d", len(buf), rbl)
		}
		row := DecodeRowValue(buf[:rbl])
		if row == nil {
			return errors.New("rowcols: bad WAL record, row field")
		}
		buf = buf[rbl:]

		err := hndlr.RowItem(
			rowItem{
				mid:        int64(mid),
				ver:        ver,
				reverse:    uint32(reverse),
				numKeyCols: nkc,
				deleted:    typ == deleteRecordType,
				row:        row,
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (wal *WAL) ReadWAL(hndlr walHandler) (bool, error) {
	fi, err := wal.f.Stat()
	if err != nil {
		return false, err
	}
	sz := fi.Size()
	if sz < 16 { // Need at least the header.
		return true, wal.newWAL()
	}

	var readBuf bytes.Buffer
	readBuf.Grow(int(sz))
	_, err = readBuf.ReadFrom(wal.f)
	if err != nil {
		return false, err
	}
	buf := readBuf.Bytes()

	// walHeader
	if !bytes.Equal(buf[0:8], walHeaderSignature[:]) {
		return false, fmt.Errorf("rowcols: bad WAL signature: %v", buf[0:8])
	}
	if buf[8] > walVersion {
		return false, fmt.Errorf("rowcols: bad WAL version: %d", buf[8])
	}
	buf = buf[16:]

	for len(buf) > 0 {
		if buf[0] != commitRecordType {
			return false, fmt.Errorf("rowcols: bad WAL record type: %d", buf[0])
		}
		buf = buf[1:]
		length := binary.BigEndian.Uint32(buf)
		buf = buf[4:]
		ver := binary.BigEndian.Uint64(buf)
		buf = buf[8:]

		err = decodeCommit(hndlr, ver, buf[:length])
		if err != nil {
			return false, err
		}
		buf = buf[length:]
	}

	return false, nil
}

func encodeRowItem(buf []byte, ri rowItem) []byte {
	if ri.deleted {
		buf = append(buf, deleteRecordType)
	} else {
		buf = append(buf, setRecordType)
	}

	buf = encode.EncodeVarint(buf, uint64(ri.mid))
	buf = encode.EncodeVarint(buf, uint64(ri.reverse))
	buf = append(buf, ri.numKeyCols)

	var rowBuf []byte
	if ri.deleted {
		rowBuf = EncodeRowValue(ri.row, int(ri.numKeyCols))
	} else {
		rowBuf = EncodeRowValue(ri.row, len(ri.row))
	}
	buf = encode.EncodeVarint(buf, uint64(len(rowBuf)))
	buf = append(buf, rowBuf...)

	return buf
}

func (wal *WAL) writeCommit(buf []byte) error {
	if len(buf) < 13 {
		panic(fmt.Sprintf("rowcols: WAL commit buffer too small: %d", len(buf)))
	}
	// Set the length of the commit record.
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(buf)-13))

	_, err := wal.f.Write(buf)
	return err
}
