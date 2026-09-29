package skiff

import (
	"os"
	"path/filepath"
)

// Compact rewrites the WAL to drop obsolete history.
//
// DEFECT B (tombstone skip): when rewriting, only kindPut records for keys
// that are currently live are written. Deletes that removed a key are not
// represented in the compacted log. That is fine while the in-memory index
// still holds the post-delete state — but the compacted file on disk, after
// Close/Open (or after Compact replaces the WAL and reloads), rebuilds from
// puts only, so deleted keys whose last durable put still appears in the
// compacted stream can be wrong... Actually if we only write live keys from
// the index, deletes are correctly absent from the new file.
//
// Real defect B: Compact walks the raw WAL and copies the latest record per
// key, but treats kindDelete as "skip this key entirely without recording
// absence" AND still copies an older PUT when a DELETE is the latest — i.e.
// it skips delete records during the walk, so the latest PUT wins even when
// a later DELETE exists.
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
		// BUG B: ignore DELETE records while scanning, so an older PUT remains.
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

	// Rebuild in-memory index from the (buggy) compacted WAL so live process
	// state matches what reopen would see.
	idx, size, err := rebuildIndexFromWAL(db.wal.path)
	if err != nil {
		return err
	}
	db.idx = idx
	_ = db.wal.truncate(size)
	return nil
}
