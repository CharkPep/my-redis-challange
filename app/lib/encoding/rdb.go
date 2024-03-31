package encoding

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
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
)

type Rdb struct {
	logger   *log.Logger
	keys     map[string]*storage.StringsStorage
	version  []byte
	metadata map[string]string
}

func NewRdb() *Rdb {
	logger := log.New(os.Stdout, "RDB ", log.LstdFlags)
	return &Rdb{
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

	buff := make([]byte, length, length+1)
	_, err = r.Read(buff)
	if err != nil {
		return err
	}

	rdb.logger.Printf("Unmarshalling RDB: %s", buff)
	if _, err = rdb.Load(bufio.NewReader(bytes.NewBuffer(buff))); err != nil {
		return err
	}
	return nil
}

func (rd *Rdb) Apply(storage *storage.StringsStorage) {
	for _, keys := range rd.keys {
		for _, k := range keys.Keys(regexp.MustCompile(".*")) {
			v, _ := keys.Get(k)
			storage.Set(k, v, time.Time{})
		}
	}
}

func (rd *Rdb) Load(r *bufio.Reader) (N int, err error) {
	var (
		n     int
		start = time.Now()
	)

	N, _ = r.Discard(len(MAGICSTRING))
	if n, err = r.Read(rd.version); err != nil {
		return n, err
	}

	N += n
	rd.logger.Printf("RDB version: %s", rd.version)
	if err = peekAndAssert(r, []byte{METADATA}); err != nil {
		return
	}

	metadata, err := r.ReadSlice(DB)
	if err := r.UnreadByte(); err != nil {
		return N, err
	}

	if err != nil {
		return
	}

	mReader := bufio.NewReader(bytes.NewBuffer(metadata[:len(metadata)-1]))
	for err := peekAndAssert(mReader, []byte{METADATA}); err != nil; err = peekAndAssert(mReader, []byte{METADATA}) {
		mReader.Discard(1)
		key, err := DecodeString(mReader)
		if err != nil {
			rd.logger.Printf("Error decoding metadata key: %s", err)
			return N, err
		}

		value, err := DecodeString(mReader)
		if err != nil {
			return N, err
		}

		rd.metadata[key] = value
	}

	if err = rd.readDb(r); err != nil {
		return
	}

	rd.logger.Printf("Done parsing RDB file in %s", time.Since(start))
	rd.logger.Printf("Keys: %v, %d", rd.keys, len(rd.keys))
	return
}

func (rd *Rdb) readDb(r *bufio.Reader) error {
	for db, resize, expire, err := rd.assertAndReadDbMetadata(r); err == nil; db, resize, expire, err = rd.assertAndReadDbMetadata(r) {
		rd.logger.Printf("Reading DB: %d", db)
		rd.keys = make(map[string]*storage.StringsStorage)
		if err = rd.readDbKeys(r, db, resize, expire); err != nil {
			return err
		}
	}

	return nil
}

func (rd *Rdb) readDbKeys(r *bufio.Reader, db byte, resize, expire uint32) error {
	if rd.keys[string(db)] == nil {
		rd.keys[string(db)] = storage.New(nil)
	}

	for {
		b, err := r.Peek(1)
		if err != nil {
			return err
		}

		r.Discard(1)
		switch b[0] {
		case EOF, DB:
			return nil
		case EXPIRETIME, EXPIRETIMEMS:
			kvExpire := make([]byte, 4)
			if _, err := r.Read(kvExpire); err != nil {
				return err
			}
			// skip value type, suppose all KVs are strings
			r.Discard(1)
		}

		key, err := DecodeString(r)
		if err != nil {
			return err
		}

		value, err := DecodeString(r)
		if err != nil {
			return err
		}

		rd.logger.Printf("DB: %d, Key: %s, Value: %s", db, key, value)
		rd.keys[string(db)].Set(key, value, time.Time{})

	}
}

func (rd *Rdb) assertAndReadDbMetadata(r *bufio.Reader) (db byte, resize uint32, expire uint32, err error) {
	if err = peekAndAssert(r, []byte{DB}); err != nil {
		return
	}

	r.Discard(1)
	rd.logger.Printf("Reading DB metadata\n")
	db, err = r.ReadByte()
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
