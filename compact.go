package skiff

import (
	"os"
	"path/filepath"
)

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

	latest := foldLatest(b)

	tmp := filepath.Join(db.dir, "skiff.wal.compact")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, s := range latest {
		rec := encodeRecord(kindPut, s.key, s.value)
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
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	_ = f.Close()

	if err := writeWatermark(db.dir, st.Size()); err != nil {
		_ = os.Remove(tmp)
		return err
	}

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
	_ = writeWatermark(db.dir, size)
	return nil
}
