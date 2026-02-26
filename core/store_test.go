package core

import (
	"path/filepath"

	"testing"
)

func TestPutGet(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	wal, err := NewWal(walPath)

	if err != nil {
		t.Fatalf("failed to create wal : %v", err)
	}
	defer wal.Close()
	store := NewStore(wal)

	key := "k1"
	value := []byte("orange")

	if err := store.Put(key, value); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	v, err := store.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	} else if string(v) != "orange" {
		t.Fatalf("value mismatch: expected %s, got %s", "orange", v)
	}

}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	wal, err := NewWal(walPath)

	if err != nil {
		t.Fatalf("failed to create wal : %v", err)
	}
	defer wal.Close()
	store := NewStore(wal)

	_ = store.Put("k1", []byte("orange"))
	if err := store.Delete("k1"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := store.Get("k1"); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got: %v", err)
	}

}
func TestRecoverFromWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	// first execution
	{
		wal, _ := NewWal(walPath)
		store := NewStore(wal)

		_ = store.Put("a", []byte("100"))
		_ = store.Put("b", []byte("200"))
		// t.Log("state 1: ",store.storage)
		_ = store.Delete("a")
		wal.Close()
		// t.Log("state 2: ",store.storage)
	}

	// recovery
	{
		wal, _ := NewWal(walPath)
		store := NewStore(wal)
		defer wal.Close()
		if err := store.RecoverFromWAL(); err != nil {
			t.Fatalf("recovery failed: %v", err)
		}
		// t.Log("state 3: ",store.storage)
		if _, err := store.Get("a"); err == nil {
			t.Fatalf("expected 'a' to be deleted")
		}

		v, err := store.Get("b")
		if err != nil || string(v) != "200" {
			t.Fatalf("unexpected value for b: %v", v)
		}
	}
}
func TestByteCorrectness(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")

	wal, _ := NewWal(walPath)
	store := NewStore(wal)
	defer wal.Close()
	val := []byte{0x00, 0xff, 0x10, 0x13, 0xA3, 0x7D, 0x42, 0x11, 0x99, 0xCC, 0x23}
	_ = store.Put("binary-val", val)

	v, _ := store.Get("binary-val")

	if len(v) != len(val) {
		t.Fatalf("length mismatch")
	}
	for i := range val {
		if v[i] != val[i] {
			t.Fatalf("byte mismatch at %d", i)
		}
	}
}
