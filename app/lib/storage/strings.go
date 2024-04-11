package storage

import (
	"regexp"
	"sync"
	"time"
)

const PICK_NUMBER = 20

type StringsElement struct {
	Value  string
	Expire time.Time
}

type StringsDataType struct {
	storage map[string]StringsElement
	mu      *sync.RWMutex
}

func (s *StringsDataType) cleanExpireKeys() {
	cur := 0
	for key, elem := range s.storage {
		if elem.Expire.Before(time.Now()) && !elem.Expire.IsZero() {
			delete(s.storage, key)
		}

		cur++
		if cur <= PICK_NUMBER {
			return
		}
	}
}

func (s *StringsDataType) startExpiryWorker() {
	for {
		time.Sleep(time.Second * 6)
		s.cleanExpireKeys()
	}
}

func (s *StringsDataType) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.storage, key)
}

func NewStringsStorage() *StringsDataType {
	return NewStringsStorageFromExists(nil)
}

func NewStringsStorageFromExists(cache map[string]StringsElement) *StringsDataType {
	if cache == nil {
		cache = make(map[string]StringsElement)
	}

	stringStorage := &StringsDataType{
		storage: cache,
		mu:      &sync.RWMutex{},
	}

	//go stringStorage.startExpiryWorker()
	return stringStorage
}

func (s *StringsDataType) Set(key string, value string, expire time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage[key] = StringsElement{
		Value:  value,
		Expire: expire,
	}
}

func (s *StringsDataType) Get(key string) (string, bool) {
	s.mu.RLock()
	elem, ok := s.storage[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}

	if elem.Expire.Before(time.Now()) && !elem.Expire.IsZero() {
		s.Delete(key)
		return "", false
	}

	return elem.Value, true
}

func (s *StringsDataType) Keys(pattern *regexp.Regexp) []string {
	keys := make([]string, 0, len(s.storage))
	for key, elem := range s.storage {
		if elem.Expire.Before(time.Now()) && !elem.Expire.IsZero() {
			delete(s.storage, key)
			continue
		}
		if pattern == nil || pattern.MatchString(key) {
			keys = append(keys, key)
		}
	}

	return keys
}

func (s *StringsDataType) GetType() StorageType {
	return STRINGS
}

func (s *StringsDataType) Cp(other *StringsDataType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range other.storage {
		s.storage[k] = v
	}
}
