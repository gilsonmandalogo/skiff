package skiff

import (
	"encoding/binary"
	"io"
	"os"
)

// Backup writes a snapshot of the current keyspace to w.
func (db *DB) Backup(w io.Writer) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	snap := db.idx.snapshot()
	// header: count u32
	keys := make([]string, 0, len(snap))
	for k, v := range snap {
		if len(v) == 0 {
			continue
		}
		keys = append(keys, k)
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(keys)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	for _, k := range keys {
		v := snap[k]
		var lens [8]byte
		binary.LittleEndian.PutUint32(lens[0:4], uint32(len(k)))
		binary.LittleEndian.PutUint32(lens[4:8], uint32(len(v)))
		if _, err := w.Write(lens[:]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(k)); err != nil {
			return err
		}
		if _, err := w.Write(v); err != nil {
			return err
		}
	}
	return nil
}

// Restore replaces the database contents with a snapshot previously written
// by Backup. The database must be open; existing keys are cleared first.
func (db *DB) Restore(r io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := int(binary.LittleEndian.Uint32(hdr[:]))
	fresh := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		var lens [8]byte
		if _, err := io.ReadFull(r, lens[:]); err != nil {
			return err
		}
		klen := int(binary.LittleEndian.Uint32(lens[0:4]))
		vlen := int(binary.LittleEndian.Uint32(lens[4:8]))
		kb := make([]byte, klen)
		if _, err := io.ReadFull(r, kb); err != nil {
			return err
		}
		vb := make([]byte, vlen)
		if _, err := io.ReadFull(r, vb); err != nil {
			return err
		}
		fresh[string(kb)] = vb
	}

	tmp := db.wal.path + ".restore"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for k, v := range fresh {
		rec := encodeRecord(kindPut, []byte(k), v)
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
	db.idx.replace(fresh)
	return nil
}
