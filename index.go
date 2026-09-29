package skiff

import "sync"

// index maps key -> value. Deletes remove the key. Empty values are stored as
// a present entry with a zero-length slice (not the same as absent).
type index struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newIndex() *index {
	return &index{data: make(map[string][]byte)}
}

func (idx *index) put(key, value []byte) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	idx.data[string(key)] = cp
}

func (idx *index) delete(key []byte) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.data, string(key))
}

func (idx *index) get(key []byte) ([]byte, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	v, ok := idx.data[string(key)]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

func (idx *index) has(key []byte) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.data[string(key)]
	return ok
}

func (idx *index) snapshot() map[string][]byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make(map[string][]byte, len(idx.data))
	for k, v := range idx.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func (idx *index) replace(data map[string][]byte) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.data = data
}

func (idx *index) len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.data)
}
