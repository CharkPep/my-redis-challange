package storage

import (
	"github.com/armon/go-radix"
)

type StreamDataType struct {
	name string
	// For use case here the ideal would be to implement new radix tree optimized for
	// range queries(iteration from subtree to root to leafs that are located after current leaf) and blocking behavior,
	// though I decided to use already implemented tree
	tree *radix.Tree
}

func NewStream(stream string) *StreamDataType {
	return &StreamDataType{
		name: stream,
		tree: radix.New(),
	}
}

func (st StreamDataType) GetType() DataType {
	return STREAMS
}

func (st StreamDataType) Add(key string, data interface{}) (old interface{}, ok bool) {
	return st.tree.Insert(key, data)
}

func (st StreamDataType) Min() (key string, val interface{}, ok bool) {
	return st.tree.Minimum()
}

func (st StreamDataType) Max() (key string, val interface{}, ok bool) {
	return st.tree.Maximum()
}
