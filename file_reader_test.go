package butteredscones

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestLineReaderReadingFileWithFields(t *testing.T) {
	file, err := os.Open("fixtures/basic.log")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewFileReader(file, map[string]string{"type": "syslog"}, 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case chunk := <-reader.C:
		if chunk[0].Data["line"] != "line1" {
			t.Fatalf("Expected \"line1\", got %q", chunk[0].Data["line"])
		}
		if chunk[0].Data["type"] != "syslog" {
			t.Fatalf("Expected \"type\":\"syslog\", got %q", chunk[0].Data["type"])
		}
		if chunk[0].HighWaterMark.Position != 6 {
			t.Fatalf("Expected HighWaterMark.Position=6, got %d", chunk[0].HighWaterMark.Position)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}

	select {
	case chunk := <-reader.C:
		if chunk[0].Data["line"] != "line2" {
			t.Fatalf("Expected \"line2\", got %q", chunk[0].Data["line"])
		}
		if chunk[0].Data["type"] != "syslog" {
			t.Fatalf("Expected \"type\":\"syslog\", got %q", chunk[0].Data["type"])
		}
		if chunk[0].HighWaterMark.Position != 12 {
			t.Fatalf("Expected HighWaterMark.Position=12, got %d", chunk[0].HighWaterMark.Position)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}

	select {
	case _, ok := <-reader.C:
		if ok {
			t.Fatalf("Expected channel to be closed after EOF, but was not")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}
}

func TestLineReaderReadingWindowsEndings(t *testing.T) {
	file, err := os.Open("fixtures/windows.log")
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewFileReader(file, map[string]string{"type": "syslog"}, 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case chunk := <-reader.C:
		if chunk[0].Data["line"] != "line1" {
			t.Fatalf("Expected \"line1\", got %q", chunk[0].Data["line"])
		}
		if chunk[0].Data["type"] != "syslog" {
			t.Fatalf("Expected \"type\":\"syslog\", got %q", chunk[0].Data["type"])
		}
		if chunk[0].HighWaterMark.Position != 7 {
			t.Fatalf("Expected HighWaterMark.Position=7, got %d", chunk[0].HighWaterMark.Position)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}

	select {
	case chunk := <-reader.C:
		if chunk[0].Data["line"] != "line2" {
			t.Fatalf("Expected \"line2\", got %q", chunk[0].Data["line"])
		}
		if chunk[0].Data["type"] != "syslog" {
			t.Fatalf("Expected \"type\":\"syslog\", got %q", chunk[0].Data["type"])
		}
		if chunk[0].HighWaterMark.Position != 14 {
			t.Fatalf("Expected HighWaterMark.Position=14, got %d", chunk[0].HighWaterMark.Position)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}

	select {
	case _, ok := <-reader.C:
		if ok {
			t.Fatalf("Expected channel to be closed after EOF, but was not")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}
}

func TestLineReaderPartialLine(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "butteredscones")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// We write a complete line, then a partial line. FileReader is supposed to
	// read one line successfully, EOF without the partial line sent.
	_, err = tmpFile.Write([]byte("line1\npartial line"))
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	reader, err := NewFileReader(file, map[string]string{"type": "syslog"}, 1)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case chunk := <-reader.C:
		if chunk[0].Data["line"] != "line1" {
			t.Fatalf("Expected \"line1\", got %q", chunk[0].Data["line"])
		}
		if chunk[0].Data["type"] != "syslog" {
			t.Fatalf("Expected \"type\":\"syslog\", got %q", chunk[0].Data["type"])
		}
		if chunk[0].HighWaterMark.Position != 6 {
			t.Fatalf("Expected HighWaterMark.Position=6, got %d", chunk[0].HighWaterMark.Position)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}

	select {
	case _, ok := <-reader.C:
		if ok {
			t.Fatalf("Expected channel to be closed after EOF, but was not")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Timeout")
	}
}
