package skiff

import (
	"os"
	"sync"
)

// DB is a small Bitcask-style durable key/value store.
//
// Values may be empty (length 0). An empty value is not the same as a missing
// key: Has is true and Get returns a zero-length slice with a nil error.
type DB struct {
	mu     sync.RWMutex
	dir    string
	wal    *wal
	idx    *index
	closed bool
}

// Open opens (or creates) a database in dir.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := dir + "/" + walName
	idx, size, err := rebuildIndexFromWAL(path)
	if err != nil {
		return nil, err
	}
	w, err := openWAL(dir)
	if err != nil {
		return nil, err
	}
	if err := w.truncate(size); err != nil {
		_ = w.close()
		return nil, err
	}
	return &DB{dir: dir, wal: w, idx: idx}, nil
}

// Close flushes and closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.wal.sync(); err != nil {
		_ = db.wal.close()
		return err
	}
	return db.wal.close()
}

// Sync fsyncs the WAL.
func (db *DB) Sync() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.wal.sync()
}

// Put stores value under key. An empty value is a stored empty blob.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := db.wal.append(kindPut, key, value); err != nil {
		return err
	}
	db.idx.put(key, value)
	return nil
}

// Delete removes key. After a successful Delete, Has is false and Get returns ErrNotFound.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if err := db.wal.append(kindDelete, key, nil); err != nil {
		return err
	}
	db.idx.delete(key)
	return nil
}

// Get returns the value for key. Missing keys yield ErrNotFound.
// A stored empty value yields a zero-length slice and a nil error.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	v, ok := db.idx.get(key)
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// Has reports whether key is present (including when the stored value is empty).
func (db *DB) Has(key []byte) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return false
	}
	return db.idx.has(key)
}

// Len returns the number of live keys.
func (db *DB) Len() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.idx.len()
}
