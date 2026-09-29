package skiff

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
	if err != nil || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("get: %q %v", v, err)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("a")); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if db.Has([]byte("a")) {
		t.Fatal("Has after delete")
	}
}

func TestCleanReopenPreservesDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("k"), []byte("v"))
	_ = db.Delete([]byte("k"))
	_ = db.Put([]byte("live"), []byte("ok"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("k")); err != ErrNotFound {
		t.Fatalf("deleted key after clean reopen: %v", err)
	}
	v, err := db2.Get([]byte("live"))
	if err != nil || !bytes.Equal(v, []byte("ok")) {
		t.Fatalf("live: %q %v", v, err)
	}
}

func TestEmptyValueLive(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("e"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if !db.Has([]byte("e")) {
		t.Fatal("Has empty")
	}
	v, err := db.Get([]byte("e"))
	if err != nil || v == nil || len(v) != 0 {
		t.Fatalf("empty get: %q %v", v, err)
	}
	if _, err := db.Get([]byte("missing")); err != ErrNotFound {
		t.Fatalf("missing: %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_ = db.Put([]byte("k"), []byte("old"))
	_ = db.Put([]byte("k"), []byte("new"))
	v, _ := db.Get([]byte("k"))
	if !bytes.Equal(v, []byte("new")) {
		t.Fatalf("got %q", v)
	}
}

func TestSyncCreatesWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("k"), []byte("v"))
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := os.Stat(filepath.Join(dir, walName)); err != nil {
		t.Fatal(err)
	}
}
