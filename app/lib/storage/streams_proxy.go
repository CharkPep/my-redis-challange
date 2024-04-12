package storage

import (
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

func (st StreamProxy) Add(key string, data interface{}) (old interface{}, ok bool) {
	st.kType.SetType(st.stream.name, STREAMS)
	return st.stream.Add(key, data)
}
