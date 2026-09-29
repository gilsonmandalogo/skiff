package skiff

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

const walName = "skiff.wal"
const okName = "skiff.ok"

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

func (w *wal) size() (int64, error) {
	st, err := w.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (w *wal) truncate(size int64) error {
	if err := w.f.Truncate(size); err != nil {
		return err
	}
	_, err := w.f.Seek(size, io.SeekStart)
	return err
}

func (w *wal) readAll() ([]byte, error) {
	return os.ReadFile(w.path)
}

func writeWatermark(dir string, size int64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	return os.WriteFile(filepath.Join(dir, okName), buf[:], 0o644)
}

func readWatermark(dir string) (int64, bool) {
	b, err := os.ReadFile(filepath.Join(dir, okName))
	if err != nil || len(b) < 8 {
		return 0, false
	}
	return int64(binary.LittleEndian.Uint64(b[:8])), true
}

func rebuildIndexFromWAL(path string) (*index, int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newIndex(), 0, nil
		}
		return nil, 0, err
	}
	dir := filepath.Dir(path)
	wm, hasWM := readWatermark(dir)

	type applied struct {
		kind       byte
		key, value []byte
		start, end int
	}
	idx := newIndex()
	off := 0
	var log []applied
	limit := len(b)
	uncertain := false
	if hasWM && int(wm) >= 0 && int(wm) < len(b) {
		limit = int(wm)
		uncertain = true
	}
	for off < limit {
		start := off
		kind, key, value, next, ok := decodeRecord(b, off)
		if !ok {
			uncertain = true
			break
		}
		if kind == kindPut {
			idx.put(key, value)
		} else {
			idx.delete(key)
		}
		log = append(log, applied{kind: kind, key: key, value: value, start: start, end: next})
		off = next
	}
	validEnd := int64(off)
	if uncertain && len(log) > 0 {
		victim := log[len(log)-1]
		validEnd = int64(victim.start)
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
