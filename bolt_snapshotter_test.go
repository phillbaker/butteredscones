package butteredscones

import (
	"github.com/boltdb/bolt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestBoltSnapshotter(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "butteredscones")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	db, err := bolt.Open(tmpFile.Name(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	var snapshotter Snapshotter
	snapshotter = &BoltSnapshotter{DB: db}

	// Default is 0
	highWaterMark, err := snapshotter.HighWaterMark("/tmp/foo")
	if err != nil {
		t.Fatal(err)
	}
	if highWaterMark.FilePath != "/tmp/foo" {
		t.Fatalf("Expected FilePath=%q, but got %q", "/tmp/foo", highWaterMark.FilePath)
	}
	if highWaterMark.Position != 0 {
		t.Fatalf("Expected Position=%d, but got %d", 0, highWaterMark.Position)
	}

	// Set
	err = snapshotter.SetHighWaterMarks([]*HighWaterMark{
		&HighWaterMark{FilePath: "/tmp/foo", Position: 10245},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve the value we just stored
	highWaterMark, err = snapshotter.HighWaterMark("/tmp/foo")
	if err != nil {
		t.Fatal(err)
	}
	if highWaterMark.FilePath != "/tmp/foo" {
		t.Fatalf("Expected FilePath=%q, but got %q", "/tmp/foo", highWaterMark.FilePath)
	}
	if highWaterMark.Position != 10245 {
		t.Fatalf("Expected Position=%d, but got %d", 10245, highWaterMark.Position)
	}
}
