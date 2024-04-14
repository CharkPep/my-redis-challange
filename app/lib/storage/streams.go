package storage

import (
	"fmt"
	"github.com/armon/go-radix"
	"strings"
	"sync"
)

type StreamDataType struct {
	name string
	// For use case here the ideal would be to implement new radix tree optimized for
	// range queries(iteration from subtree to root to leafs that are located after current leaf) and blocking behavior,
	// though I decided to use already implemented tree
	tree *radix.Tree
	mu   *sync.RWMutex
}

type StreamKV struct {
	Key  string
	Data []string
}

func NewStream(stream string) *StreamDataType {
	return &StreamDataType{
		mu:   &sync.RWMutex{},
		name: stream,
		tree: radix.New(),
	}
}

func (st StreamDataType) GetType() DataType {
	return STREAMS
}

func (st StreamDataType) Add(key string, data []string) (old interface{}, ok bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.tree.Insert(key, data)
}

func (st StreamDataType) Min(prefix string) (key string, val interface{}) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	// as any walk function does not provide api to chose edges from the edge list, has to walk the whole prefix,
	st.tree.WalkPrefix(prefix, func(s string, v interface{}) bool {
		if strings.Compare(key, s) == -1 {
			key = s
			val = v
		}
		return false
	})

	return
}

func (st StreamDataType) Max(prefix string) (key string, val interface{}) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	// as any walk function does not provide api to chose edges from the edge list, has to walk the whole prefix
	//Key, val, ok = st.tree.LongestPrefix(prefix)
	st.tree.WalkPrefix(prefix, func(s string, v interface{}) bool {
		fmt.Printf("Walking prefix %s, current %s\n", key, s)
		if strings.Compare(fmt.Sprintf("%s-0", key), s) == -1 {
			key = s
			val = v
		}

		return false
	})

	return
}

func (st StreamDataType) Range(start, end string) []StreamKV {
	var kv []StreamKV
	st.mu.RLock()
	defer st.mu.RUnlock()
	// very slow approach, though to optimize need to implement radix tree or tweak existing))
	st.tree.Walk(func(s string, v interface{}) bool {
		if strings.Compare(s, start) == 1 && strings.Compare(s, end) == -1 || strings.Compare(s, start) == 0 || strings.Compare(s, end) == 0 {
			kv = append(kv, StreamKV{
				Key:  s,
				Data: v.([]string),
			})
		}

		return false
	})

	return kv
}
