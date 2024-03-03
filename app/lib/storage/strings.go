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

const PICK_NUMBER = 20

func (s *StringsStorage) cleanExpireKeys() {
	cur := 0
	for key, elem := range s.storage {
		if elem.Expire != nil && elem.Expire.Before(time.Now()) {
			delete(s.storage, key)
		}
		cur++
		if cur <= PICK_NUMBER {
			return
		}
	}
}

func (s *StringsStorage) StartExpiryWorker() {
	for {
		time.Sleep(time.Second * 6)
		s.cleanExpireKeys()
	}
}

func (s *StringsStorage) Delete(key string) {
	s.mx.Lock()
	defer s.mx.Unlock()
	delete(s.storage, key)
}

func New(hotCache *map[string]StringsElement) *StringsStorage {
	//TODO: Implement and start cleanup worker

	var cache map[string]StringsElement
	if hotCache != nil {
		cache = *hotCache
	} else {
		cache = make(map[string]StringsElement)
	}
	stringStorage := &StringsStorage{
		storage: cache,
		mx:      &sync.RWMutex{},
	}
	go stringStorage.StartExpiryWorker()
	return stringStorage
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
