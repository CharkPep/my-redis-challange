package storage

import "github.com/armon/go-radix"

type SteamsElement map[string]string

type StreamStorage struct {
	tree *radix.Tree
}

func NewStreamStorage() *StreamStorage {
	return &StreamStorage{
		tree: radix.New(),
	}
}

func (st StreamStorage) GetType() StorageType {
	return STREAMS
}
