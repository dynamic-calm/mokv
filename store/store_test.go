package store_test

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/store"
)

func TestNew(t *testing.T) {
	t.Parallel()

	if s := store.New(); s == nil {
		t.Fatal("store.New() returned nil")
	}
}

func TestStore_SetAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		value []byte
	}{
		{
			name:  "simple string value",
			key:   "key1",
			value: []byte("value1"),
		},
		{
			name:  "empty value",
			key:   "key2",
			value: []byte{},
		},
		{
			name:  "nil value",
			key:   "key3",
			value: nil,
		},
		{
			name:  "binary data",
			key:   "key4",
			value: []byte{0x00, 0xFF, 0x10, 0xAB},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New()

			// Set the value
			if err := s.Set(tt.key, tt.value, time.Time{}); err != nil {
				t.Fatalf("Set() error = %v", err)
			}

			// Get the value
			got, err := s.Get(tt.key)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}

			// Compare values
			if !bytes.Equal(got, tt.value) {
				t.Errorf("Get() = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestStore_Get_NotFound(t *testing.T) {
	t.Parallel()

	s := store.New()

	_, err := s.Get("nonexistent")
	if err == nil {
		t.Error("Get() expected error for nonexistent key, got nil")
	}
}

func TestStore_Update(t *testing.T) {
	t.Parallel()

	s := store.New()
	key := "updateKey"

	// Set initial value
	initialValue := []byte("initial")
	if err := s.Set(key, initialValue, time.Time{}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Update value
	updatedValue := []byte("updated")
	if err := s.Set(key, updatedValue, time.Time{}); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Verify updated value
	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if !bytes.Equal(got, updatedValue) {
		t.Errorf("Get() = %v, want %v", got, updatedValue)
	}
}

func TestStore_Delete(t *testing.T) {
	t.Parallel()

	t.Run("delete existing key", func(t *testing.T) {
		s := store.New()
		key := "deleteMe"
		value := []byte("value")

		// Set a value
		if err := s.Set(key, value, time.Time{}); err != nil {
			t.Fatalf("Set() error = %v", err)
		}

		// Delete it
		if err := s.Delete(key); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}

		// Verify it's gone
		_, err := s.Get(key)
		if err == nil {
			t.Error("Get() expected error after delete, got nil")
		}
	})

	t.Run("delete nonexistent key", func(t *testing.T) {
		s := store.New()

		// Delete should not error for nonexistent key
		if err := s.Delete("nonexistent"); err != nil {
			t.Errorf("Delete() unexpected error = %v", err)
		}
	})
}

func TestStore_List(t *testing.T) {
	t.Parallel()

	t.Run("empty store", func(t *testing.T) {
		s := store.New()

		var count int
		for range s.List() {
			count++
		}

		if count != 0 {
			t.Errorf("List() returned %d items, want 0", count)
		}
	})

	t.Run("multiple items", func(t *testing.T) {
		s := store.New()

		testData := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		}

		// Add test data
		for k, v := range testData {
			if err := s.Set(k, v, time.Time{}); err != nil {
				t.Fatalf("Set() error = %v", err)
			}
		}

		// Collect all values from List
		seen := make(map[string]bool)
		for val := range s.List() {
			seen[string(val)] = true
		}

		// Verify count
		if len(seen) != len(testData) {
			t.Errorf("List() returned %d items, want %d", len(seen), len(testData))
		}

		// Verify all values are present
		for _, v := range testData {
			if !seen[string(v)] {
				t.Errorf("List() missing value %q", v)
			}
		}
	})
}

func TestStore_Concurrent(t *testing.T) {
	t.Parallel()

	s := store.New()
	const goroutines = 100
	const operations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Concurrent writes
	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			for j := range operations {
				key := string(rune('A' + (id % 26)))
				value := []byte{byte(id), byte(j)}
				if err := s.Set(key, value, time.Time{}); err != nil {
					t.Errorf("Set() error = %v", err)
				}
			}
		}(i)
	}

	// Concurrent reads
	wg.Add(goroutines)
	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			for range operations {
				key := string(rune('A' + (id % 26)))
				_, _ = s.Get(key) // May or may not exist
			}
		}(i)
	}

	// Concurrent deletes
	wg.Add(goroutines / 2)
	for i := range goroutines / 2 {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				key := string(rune('A' + (id % 26)))
				if err := s.Delete(key); err != nil {
					t.Errorf("Delete() error = %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// If we got here without panicking or data races, test passes
}

func TestStore_ConcurrentList(t *testing.T) {
	t.Parallel()

	s := store.New()

	// Add some initial data
	for i := range 10 {
		key := string(rune('A' + i))
		value := []byte{byte(i)}
		if err := s.Set(key, value, time.Time{}); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}

	var wg sync.WaitGroup

	// Concurrent List operations
	wg.Add(5)
	for range 5 {
		go func() {
			defer wg.Done()
			count := 0
			for range s.List() {
				count++
			}
		}()
	}

	// Concurrent modifications while listing
	wg.Add(5)
	for i := range 5 {
		go func(id int) {
			defer wg.Done()
			key := string(rune('Z' - id))
			value := []byte{byte(id)}
			_ = s.Set(key, value, time.Time{})
		}(i)
	}

	wg.Wait()
}

func TestStore_Expiry(t *testing.T) {
	t.Parallel()

	t.Run("expired key is hidden and reported", func(t *testing.T) {
		s := store.New()
		expiresAt := time.Now().Add(-time.Second)
		if err := s.Set("old", []byte("old"), expiresAt); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
		if err := s.Set("fresh", []byte("fresh"), time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("Set() error = %v", err)
		}

		if _, err := s.Get("old"); err == nil {
			t.Error("Get() expected error for expired key, got nil")
		}
		for val := range s.List() {
			if string(val) == "old" {
				t.Error("List() returned an expired value")
			}
		}

		expired := s.Expired(time.Now())
		if len(expired) != 1 || !expired["old"].Equal(expiresAt) {
			t.Errorf("Expired() = %v, want only old at %v", expired, expiresAt)
		}
	})

	t.Run("delete expired removes matching deadline", func(t *testing.T) {
		s := store.New()
		expiresAt := time.Now().Add(-time.Second)
		_ = s.Set("key", []byte("value"), expiresAt)

		if err := s.DeleteExpired("key", expiresAt); err != nil {
			t.Fatalf("DeleteExpired() error = %v", err)
		}
		if len(s.Expired(time.Now())) != 0 {
			t.Error("DeleteExpired() did not remove the key")
		}
	})

	t.Run("delete expired keeps overwritten key", func(t *testing.T) {
		s := store.New()
		expiresAt := time.Now().Add(-time.Second)
		_ = s.Set("key", []byte("old"), expiresAt)
		_ = s.Set("key", []byte("new"), time.Now().Add(time.Hour))

		if err := s.DeleteExpired("key", expiresAt); err != nil {
			t.Fatalf("DeleteExpired() error = %v", err)
		}
		got, err := s.Get("key")
		if err != nil || !bytes.Equal(got, []byte("new")) {
			t.Errorf("Get() = %q, %v, want new", got, err)
		}
	})
}
