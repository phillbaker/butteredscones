package butteredscones

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestFileReaderPoolLockUnlock(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "butteredscones")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	pool := NewFileReaderPool()
	reader, _ := NewFileReader(tmpFile, map[string]string{}, 128)
	pool.Add(reader)

	lockedReaders := make(chan *FileReader)
	go func() {
		for i := 0; i < 20; i++ {
			if lockedReader := pool.LockNext(); lockedReader != nil {
				lockedReaders <- lockedReader
			}
			<-time.After(10 * time.Millisecond)
		}
	}()

	select {
	case lockedReader := <-lockedReaders:
		if lockedReader != reader {
			t.Fatalf("Expected reader %p but got %p", reader, lockedReader)
		}
		// Attempting to grab another reader should be nil
		if anotherLockedReader := pool.LockNext(); anotherLockedReader != nil {
			t.Fatalf("Expected to get nil when locking another reader, but got %#v", anotherLockedReader)
		}
		// Unlock the reader to make it available again
		pool.Unlock(lockedReader)
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Timed out")
	}

	select {
	case lockedReader := <-lockedReaders:
		if lockedReader != reader {
			t.Fatalf("Expected reader %p but got %p", reader, lockedReader)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Timed out")
	}
}
