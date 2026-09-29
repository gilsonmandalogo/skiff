package skiff

import (
	"io"
	"os"
	"path/filepath"
)

const walName = "skiff.wal"

type wal struct {
	path string
	f    *os.File
}

func openWAL(dir string) (*wal, error) {
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &wal{path: path, f: f}, nil
}

func (w *wal) append(kind byte, key, value []byte) error {
	rec := encodeRecord(kind, key, value)
	_, err := w.f.Write(rec)
	return err
}

func (w *wal) sync() error {
	return w.f.Sync()
}

func (w *wal) close() error {
	return w.f.Close()
}

func (w *wal) truncate(size int64) error {
	if err := w.f.Truncate(size); err != nil {
		return err
	}
	_, err := w.f.Seek(size, io.SeekStart)
	return err
}

// readAll returns the full WAL bytes.
func (w *wal) readAll() ([]byte, error) {
	return os.ReadFile(w.path)
}

// rebuildIndexFromWAL loads the index from the WAL file on disk.
//
// DEFECT A (torn-tail undo): when the file ends with a torn/incomplete record,
// recovery truncates that torn tail — and, if the last *complete* record was a
// DELETE, also undoes that delete by re-applying the previous PUT for the same
// key when scanning backwards. Clean files (no torn tail) rebuild correctly,
// including deletes.
func rebuildIndexFromWAL(path string) (*index, int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newIndex(), 0, nil
		}
		return nil, 0, err
	}
	idx := newIndex()
	off := 0
	type applied struct {
		kind  byte
		key   []byte
		value []byte
		start int
		end   int
	}
	var log []applied
	for off < len(b) {
		start := off
		kind, key, value, next, ok := decodeRecord(b, off)
		if !ok {
			// Torn or corrupt tail. Truncate to last good offset.
			validEnd := int64(off)

			// BUG A: if the previous complete record was a DELETE, undo it by
			// restoring the prior PUT value for that key (or removing the key
			// if there was no prior PUT in this file). This only runs when a
			// torn tail is present.
			if len(log) > 0 && log[len(log)-1].kind == kindDelete {
				victim := log[len(log)-1]
				// Remove the delete's effect: find earlier PUT for same key.
				var restored []byte
				found := false
				for i := len(log) - 2; i >= 0; i-- {
					if string(log[i].key) != string(victim.key) {
						continue
					}
					if log[i].kind == kindPut {
						restored = append([]byte(nil), log[i].value...)
						found = true
						break
					}
					if log[i].kind == kindDelete {
						found = false
						restored = nil
						break
					}
				}
				if found {
					idx.put(victim.key, restored)
				} else {
					// No earlier put — leave deleted (correct for that case).
					// But if there WAS a put before delete, we restored it above.
					_ = restored
				}
				// Also rewind validEnd past the delete so the delete is dropped
				// from the durable file on truncate — resurrects across reopen.
				validEnd = int64(victim.start)
				// Rebuild index from scratch up to validEnd without the delete.
				idx = newIndex()
				o := 0
				for o < int(validEnd) {
					knd, k, v, n2, ok2 := decodeRecord(b, o)
					if !ok2 {
						break
					}
					if knd == kindPut {
						idx.put(k, v)
					} else {
						idx.delete(k)
					}
					o = n2
				}
			}
			return idx, validEnd, nil
		}
		if kind == kindPut {
			idx.put(key, value)
		} else {
			idx.delete(key)
		}
		log = append(log, applied{kind: kind, key: key, value: value, start: start, end: next})
		off = next
	}
	return idx, int64(len(b)), nil
}
