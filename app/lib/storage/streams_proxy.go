package storage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	stream *StreamDataType
	kType  *keyTypeMap
}

func parseStreamKey(key string) (int, int, error) {
	k := strings.Split(key, "-")
	if len(k) != 2 {
		return 0, 0, fmt.Errorf("wrong argument format")
	}

	k1, err := strconv.ParseInt(k[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	k2, err := strconv.ParseInt(k[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return int(k1), int(k2), err
}

func (st StreamProxy) Add(key string, data interface{}) (old interface{}, ok bool, err error) {
	mx, _, ok := st.stream.Max()
	fmt.Println(mx, ok)
	if ok {
		mn1, mn2, err := parseStreamKey(mx)
		if err != nil {
			return nil, false, err
		}
		k1, k2, err := parseStreamKey(key)
		if k1 <= 0 && k2 <= 0 {
			return nil, false, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}

		if k1 < mn1 || k1 == mn1 && k2 <= mn2 {
			return nil, false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

	}
	//if strings.Compare(key, mx) != 1 {
	//	return nil, false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	//}

	//if strings.Compare(key, "0-0") == -1 {
	//	return nil, false, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	//}

	st.kType.SetType(st.stream.name, STREAMS)
	old, ok = st.stream.Add(key, data)
	return
}
