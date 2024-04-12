package storage

import (
	"regexp"
	"time"
)

type StringsStorage interface {
	Get(string) (string, bool, error)
	Set(string, string, time.Time) error
	Delete(string) (bool, error)
	Keys(pattern *regexp.Regexp) []string
}

type StringsProxy struct {
	keyTypes *keyTypeMap
	storage  *StringsDataType
}

func (s *StringsProxy) Get(key string) (string, bool, error) {
	if ok, err := s.keyTypes.AssertKeyTypeOrNone(key, STRINGS); err != nil || !ok {
		return "", false, err
	}

	val, ok := s.storage.Get(key)
	if !ok {
		s.keyTypes.Delete(key)
	}

	return val, ok, nil
}

func (s *StringsProxy) Set(key string, val string, expire time.Time) error {
	if _, err := s.keyTypes.AssertKeyTypeOrNone(key, STRINGS); err != nil {
		return err
	}

	s.keyTypes.SetType(key, STRINGS)
	s.storage.Set(key, val, expire)
	return nil
}

func (s *StringsProxy) Delete(key string) (bool, error) {
	if ok, err := s.keyTypes.AssertKeyTypeOrNone(key, STRINGS); err != nil || !ok {
		return false, err
	}

	s.keyTypes.Delete(key)
	s.storage.Delete(key)
	return true, nil
}

func (s *StringsProxy) Keys(pattern *regexp.Regexp) []string {
	return s.storage.Keys(pattern)
}

func (s *StringsProxy) GetType() DataType {
	return s.storage.GetType()
}
