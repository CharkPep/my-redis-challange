package storage

import (
	"sync"
	"time"
)

type StringsElement struct {
	Value  string
	Expire *time.Time
}

type StringsStorage struct {
	storage map[string]StringsElement
	mx      *sync.RWMutex
}

func New(hotCache *map[string]StringsElement) *StringsStorage {
	//TODO: Implement and start cleanup worker
	var cache map[string]StringsElement
	if hotCache != nil {
		cache = *hotCache
	} else {
		cache = make(map[string]StringsElement)
	}
	return &StringsStorage{
		storage: cache,
		mx:      &sync.RWMutex{},
	}
}

func (s *StringsStorage) Set(key string, value string, expire *time.Time) {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.storage[key] = StringsElement{
		Value:  value,
		Expire: expire,
	}
}

func (s *StringsStorage) Get(key string) (string, bool) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	elem, ok := s.storage[key]
	if !ok {
		return "", false
	}
	if elem.Expire != nil && elem.Expire.Before(time.Now()) {
		delete(s.storage, key)
		return "", false
	}
	return elem.Value, true
}
