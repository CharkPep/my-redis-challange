package encoding

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	RDBRAW         = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	EMPTYRDBRAW, _ = base64.StdEncoding.DecodeString(RDBRAW)
	MAGICSTRING    = []byte("REDIS")
	METADATA       = byte(0xfa)
	RESIZEDB       = byte(0xfb)
	EXPIRETIMEMS   = byte(0xfc)
	EXPIRETIME     = byte(0xfd)
	DB             = byte(0xfe)
	EOF            = byte(0xff)
	STRING         = byte(0x00)
	STREAM         = byte(0x15)
)

type Rdb struct {
	logger   *log.Logger
	db       *sync.Map
	version  []byte
	metadata map[string]string
}

func NewRdb(db *sync.Map) *Rdb {
	logger := log.New(os.Stdout, "RDB ", log.LstdFlags)
	return &Rdb{
		db:       db,
		metadata: make(map[string]string),
		logger:   logger,
		version:  make([]byte, 4),
	}
}

func (rdb *Rdb) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, []byte("$")); err != nil {
		return err
	}

	if _, err := r.Discard(len([]byte("$"))); err != nil {
		return err
	}

	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return err
	}

	length, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return err
	}

	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return err
	}

	rdb.logger.Printf("Got rdb with length %d", length)
	if err = rdb.Load(r); err != nil {
		return err
	}

	return nil
}

func (rd *Rdb) Load(r *bufio.Reader) error {
	start := time.Now()
	magicString := make([]byte, len(MAGICSTRING))
	_, err := r.Read(magicString)
	if err != nil {
		err = fmt.Errorf("error reading magic string %w", err)
		return nil
	}

	if !bytes.Equal(magicString, MAGICSTRING) {
		return fmt.Errorf("error reading magic string %s", MAGICSTRING)
	}

	rd.logger.Println("Read magic string")
	if _, err = r.Read(rd.version); err != nil {
		return err
	}

	rd.logger.Printf("RDB version: %s", rd.version)
	if err = peekAndAssert(r, []byte{METADATA}); err != nil {
		return err
	}

	metadata, err := r.ReadSlice(DB)
	if err = r.UnreadByte(); err != nil {
		return err
	}

	mReader := bufio.NewReader(bytes.NewBuffer(metadata[:len(metadata)-1]))
	for err := peekAndAssert(mReader, []byte{METADATA}); err != nil; err = peekAndAssert(mReader, []byte{METADATA}) {
		mReader.Discard(1)
		key, err := DecodeString(mReader)
		if err != nil {
			rd.logger.Printf("Error decoding metadata key: %s", err)
			return err
		}

		value, err := DecodeString(mReader)
		if err != nil {
			return err
		}

		rd.metadata[key] = value
	}

	if err := rd.readDb(r); err != nil {
		return nil
	}

	r.Discard(1)
	checksum := make([]byte, 2)
	if _, err := r.Read(checksum); err != nil {
		return err
	}

	rd.logger.Printf("Done parsing RDB in %s", time.Since(start))
	return nil
}

func (rd *Rdb) readDb(r *bufio.Reader) error {
	for idx, _, _, err := rd.assertAndReadDbMetadata(r); err == nil; idx, _, _, err = rd.assertAndReadDbMetadata(r) {
		// Note redis db index max is 2^4 - 1
		dbAny, _ := rd.db.LoadOrStore(int(idx), storage.NewDb(int(idx)))
		db, ok := dbAny.(*storage.RedisDataTypes)
		if !ok {
			return fmt.Errorf("failed to assert type of db with index %d", idx)
		}

		if err = rd.readDbKeys(r, db); err != nil {
			return err
		}

	}

	return nil
}

func (rd *Rdb) readDbKeys(r *bufio.Reader, db *storage.RedisDataTypes) error {
	for {
		b, err := r.Peek(1)
		if err != nil {
			return err
		}

		vType := b[0]
		r.Discard(1)
		expireTime := time.Time{}
		switch b[0] {
		case EOF, DB:
			return nil
		case EXPIRETIME:
			kvExpire := make([]byte, 4)
			if _, err = r.Read(kvExpire); err != nil {
				return err
			}

			vType, _ = r.ReadByte()

			expireTime = time.Unix(int64(binary.LittleEndian.Uint32(kvExpire)), 0)
		case EXPIRETIMEMS:
			kvExpire := make([]byte, 8)
			if _, err = r.Read(kvExpire); err != nil {
				return err
			}

			vType, _ = r.ReadByte()
			expireTime = time.UnixMilli(int64(binary.LittleEndian.Uint64(kvExpire)))
		}

		key, err := DecodeString(r)
		if err != nil {
			return err
		}

		value, err := DecodeString(r)
		if err != nil {
			return err
		}

		// Note: if extend new types decompose into type ValueType with parse method
		switch vType {
		case STRING:
			db.GetStorage(storage.STRINGS).(storage.StringsStorage).Set(key, value, expireTime)
		default:
			rd.logger.Printf("skipping unknown type: %d", vType)
		}

	}
}

func (rd *Rdb) assertAndReadDbMetadata(r *bufio.Reader) (idx byte, resize uint32, expire uint32, err error) {
	if err = peekAndAssert(r, []byte{DB}); err != nil {
		return
	}

	r.Discard(1)
	rd.logger.Printf("Reading DB metadata\n")
	idx, err = r.ReadByte()
	if err != nil {
		return
	}

	if err = peekAndAssert(r, []byte{RESIZEDB}); err != nil {
		return
	}

	r.Discard(1)
	if resize, _, err = Decode(r); err != nil {
		return
	}

	if expire, _, err = Decode(r); err != nil {
		return
	}

	return
}

func (r *Rdb) MarshalRESP(w io.Writer) (int, error) {
	return w.Write(EMPTYRDBRAW)
}

func (r *Rdb) FullResync(w io.Writer) (int, error) {
	// For now return empty RDB
	return w.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(EMPTYRDBRAW), EMPTYRDBRAW)))
}
