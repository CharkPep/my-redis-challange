package storage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Stream interface {
	Add(key string, data interface{}) (old interface{}, ok bool)
}

type StreamsIdx struct {
	mu      *sync.RWMutex
	kTypes  *keyTypeMap
	streams map[string]*StreamProxy
}

func NewStreamIdx(kType *keyTypeMap) *StreamsIdx {
	return &StreamsIdx{
		mu:      &sync.RWMutex{},
		streams: make(map[string]*StreamProxy),
		kTypes:  kType,
	}
}

func (si StreamsIdx) GetOrCreateStream(stream string) (*StreamProxy, error) {
	if _, err := si.kTypes.AssertKeyTypeOrNone(stream, STREAMS); err != nil {
		return nil, err
	}

	si.mu.RLock()
	s, ok := si.streams[stream]
	si.mu.RUnlock()
	if !ok {
		st := NewStream(stream)
		si.mu.Lock()
		defer si.mu.Unlock()
		s = &StreamProxy{
			stream: st,
			mu:     &sync.RWMutex{},
			kType:  si.kTypes,
		}
		si.streams[stream] = s
	}

	return s, nil
}

func (si StreamsIdx) GetType() DataType {
	return STREAMS
}

type StreamProxy struct {
	mu     *sync.RWMutex
	stream *StreamDataType
	kType  *keyTypeMap
}

type StreamKey struct {
	key int64
	//	if generate true key should be ignored
	generate bool
}

func parseStreamKey(key string) (timestamp StreamKey, sequence StreamKey, err error) {
	if key == "" {
		return StreamKey{}, StreamKey{}, err
	}

	k := strings.Split(key, "-")
	if len(k) > 2 || len(k) == 1 && k[0] != "*" {
		return StreamKey{}, StreamKey{}, fmt.Errorf("wrong argument format")
	}

	if k[0] == "*" {
		timestamp.generate = true
		return StreamKey{
				generate: true,
			}, StreamKey{
				generate: true,
			}, nil
	}

	timestamp.key, err = strconv.ParseInt(k[0], 10, 64)
	if err != nil {
		return StreamKey{}, StreamKey{}, err
	}

	if k[1] == "*" {
		sequence.generate = true
	} else {
		sequence.key, err = strconv.ParseInt(k[1], 10, 64)
		if err != nil {
			return StreamKey{}, StreamKey{}, err
		}

		if timestamp.key <= 0 && sequence.key <= 0 {
			return StreamKey{}, StreamKey{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}
	}

	return
}

func (st StreamProxy) Add(k string, data interface{}) (old interface{}, key string, ok bool, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	mx, _, ok := st.stream.Max("")
	mxT, mxS, err := parseStreamKey(mx)
	if err != nil {
		return nil, k, false, err
	}

	timestamp, sequence, err := parseStreamKey(k)
	if err != nil {
		return nil, k, false, err
	}

	if timestamp.generate {
		timestamp.key = time.Now().UnixMilli()
	}

	if sequence.generate {
		fmt.Printf("Generating sequence number for %d-*\n", timestamp.key)
		mxPrefix, _, _ := st.stream.Max(fmt.Sprintf("%d-", timestamp.key))
		_, nSeq, err := parseStreamKey(mxPrefix)
		if err != nil {
			return nil, key, false, err
		}

		fmt.Printf("Longest prefix %s\n", mxPrefix)

		if mxPrefix != "" {
			sequence.key = nSeq.key + 1
		}

		// "0-*" case when tree is empty
		if timestamp.key == 0 && nSeq.key == 0 {
			sequence.key = 1
		}
	}

	if timestamp.key < mxT.key || timestamp == mxT && sequence.key <= mxS.key {
		return nil, k, false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	k = fmt.Sprintf("%d-%d", timestamp.key, sequence.key)

	st.kType.SetType(st.stream.name, STREAMS)
	old, ok = st.stream.Add(k, data)
	return nil, k, false, nil
}
