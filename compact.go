package skiff

import (
	"os"
	"path/filepath"
)

// Compact rewrites the write-ahead log, dropping obsolete history where possible.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	b, err := db.wal.readAll()
	if err != nil {
		return err
	}

	// latest[key] = encoded record bytes of the last record for that key.
	type slot struct {
		kind  byte
		key   []byte
		value []byte
	}
	latest := map[string]slot{}
	off := 0
	for off < len(b) {
		kind, key, value, next, ok := decodeRecord(b, off)
		if !ok {
			break
		}
		ks := string(key)
		if kind == kindDelete {
			off = next
			continue
		}
		latest[ks] = slot{kind: kind, key: key, value: value}
		off = next
	}

	tmp := filepath.Join(db.dir, "skiff.wal.compact")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, s := range latest {
		rec := encodeRecord(s.kind, s.key, s.value)
		if _, err := f.Write(rec); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	_ = f.Close()

	_ = db.wal.close()
	if err := os.Rename(tmp, db.wal.path); err != nil {
		return err
	}
	w, err := openWAL(db.dir)
	if err != nil {
		return err
	}
	db.wal = w

	idx, size, err := rebuildIndexFromWAL(db.wal.path)
	if err != nil {
		return err
	}
	db.idx = idx
	_ = db.wal.truncate(size)
	return nil
}
