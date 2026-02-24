package core

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestPutGet(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	wal, err := NewWal(walPath)
	defer wal.file.Close()
	if err != nil {
		t.Fatalf("failed to create wal : %v", err)
	}
	store := NewStore(wal)

	key := "k1"
	value := []byte("orange")

	if err := store.Put(key, value); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if v, err := store.Get(key); err != nil {
		t.Fatalf("get failed: %v", err)
	} else if string(v) != "orange" {
		t.Fatalf("value mismatch: expected %s, got %s", "orange", v)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal.log")
	wal, err := NewWal(walPath)
	defer wal.file.Close()
	if err != nil {
		t.Fatalf("failed to create wal : %v", err)
	}
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
		_ = store.Delete("a")
		wal.Close()
			}

	// recovery
	{
		wal, _ := NewWal(walPath)
		store := NewStore(wal)
		defer wal.Close()
		if err := store.RecoverFromWAL(); err != nil {
			t.Fatalf("recovery failed: %v", err)
		}

		if _, err := store.Get("a"); err == nil {
			t.Fatalf("expected 'a' to be deleted")
		}

		v, err := store.Get("b")
		if err != nil || string(v) != "200" {
			t.Fatalf("unexpected value for b: %v", v)
		}
	}
}
