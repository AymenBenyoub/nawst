package core

import (
	"bytes"
	"os"
	"testing"
)

func TestWal_FullRecovery(t *testing.T) {
	path := "test_full_recovery.wal"
	defer os.Remove(path)

	store := NewStore()

	wal, err := NewWal(path, 4096, AckAfterFsync)
	if err != nil {
		t.Fatal(err)
	}

	// Perform real writes
	cmds := []Command{
		{Op: OpPut, Key: "a", Value: []byte("123")},
		{Op: OpPut, Key: "b", Value: []byte("xyz")},
		{Op: OpDelete, Key: "a"},
	}

	for _, cmd := range cmds {
		done, err := wal.Append(cmd)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		if err := store.Apply(cmd); err != nil {
			t.Fatal(err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate restart
	recoveredStore := NewStore()

	if err := ReplayWal(path, recoveredStore.Apply); err != nil {
		t.Fatal(err)
	}

	// Validate state
	val, err := recoveredStore.Get("b")
	if err != nil || !bytes.Equal(val, []byte("xyz")) {
		t.Fatalf("expected b=xyz, got %v", val)
	}

	_, err = recoveredStore.Get("a")
	if err == nil {
		t.Fatalf("expected key 'a' to be deleted")
	}
}

func TestWal_OrderPreserved(t *testing.T) {
	path := "test_order.wal"
	defer os.Remove(path)

	wal, err := NewWal(path, 4096, AckAfterFsync)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		cmd := Command{
			Op:    OpPut,
			Key:   "k",
			Value: []byte{byte(i)},
		}
		done, _ := wal.Append(cmd)
		<-done
	}

	wal.Close()

	store := NewStore()
	if err := ReplayWal(path, store.Apply); err != nil {
		t.Fatal(err)
	}

	val, err := store.Get("k")
	if err != nil {
		t.Fatal(err)
	}

	if val[0] != 99 {
		t.Fatalf("expected last value 99, got %d", val[0])
	}
}

func TestWal_CrashBeforeFlush(t *testing.T) {
	path := "test_crash.wal"
	defer os.Remove(path)

	wal, err := NewWal(path, 4096, AckAfterEnqueue)
	if err != nil {
		t.Fatal(err)
	}

	done, _ := wal.Append(Command{
		Op:    OpPut,
		Key:   "a",
		Value: []byte("123"),
	})

	// Do NOT wait for done
	_ = done

	// Simulate crash (no Close)
	wal.file.Close()

	// Replay should not panic
	store := NewStore()
	_ = ReplayWal(path, store.Apply)
}
