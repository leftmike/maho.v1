package rowcols

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/encode"
	"github.com/leftmike/maho/util"
)

const (
	walVersion = 1

	commitRecordType = 1
	rowRecordType    = 2
)

var (
	walHeaderSignature = [8]byte{'m', 'a', 'h', 'o', 'w', 'a', 'l', 0}
)

type walHeader struct {
	signature  [8]byte
	walVersion byte
	unused     [7]byte
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
		if buf[0] != rowRecordType {
			return fmt.Errorf("rowcols: bad WAL record type, got %d", buf[0])
		}
		buf = buf[1:]

		var ok bool
		var rid uint64
		buf, rid, ok = util.DecodeVarint(buf)
		if !ok {
			return errors.New("rowcols: bad WAL record, rid field")
		}

		var kbl uint64
		buf, kbl, ok = util.DecodeVarint(buf)
		if !ok {
			return errors.New("rowcols: bad WAL record, key length field")
		}
		if len(buf) < int(kbl) {
			return fmt.Errorf("rowcols: bad WAL record length, have %d, want %d", len(buf), kbl)
		}
		key := buf[:kbl]
		buf = buf[kbl:]

		var rbl uint64
		buf, rbl, ok = util.DecodeVarint(buf)
		if !ok {
			return errors.New("rowcols: bad WAL record, row length field")
		}

		var row []sql.Value
		if rbl > 0 {
			if len(buf) < int(rbl) {
				return fmt.Errorf("rowcols: bad WAL record length, have %d, want %d", len(buf),
					rbl)
			}
			row = encode.DecodeRowValue(buf[:rbl])
			if row == nil {
				return errors.New("rowcols: bad WAL record, row field")
			}
			buf = buf[rbl:]
		}

		err := hndlr.RowItem(
			rowItem{
				rid: int64(rid),
				ver: ver,
				key: key,
				row: row,
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
	buf = append(buf, rowRecordType)
	buf = util.EncodeVarint(buf, uint64(ri.rid))
	buf = util.EncodeVarint(buf, uint64(len(ri.key)))
	buf = append(buf, ri.key...)
	if len(ri.row) > 0 {
		row := encode.EncodeRowValue(ri.row)
		buf = util.EncodeVarint(buf, uint64(len(row)))
		buf = append(buf, row...)
	} else {
		buf = util.EncodeVarint(buf, 0)
	}
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
