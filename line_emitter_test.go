package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestLineReaderReadingFile(t *testing.T) {
	file, err := os.Open("fixtures/basic.log")
	if err != nil {
		t.Fatal(err)
	}

	reader := NewLineReader(file, 0)
	line, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(line.Bytes, []byte("line1\n")) != 0 {
		t.Fatalf("Expected \"line1\", got %q", string(line.Bytes))
	}
	if line.Position != 0 {
		t.Fatalf("Expected position=0, got %d", line.Position)
	}

	line, err = reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(line.Bytes, []byte("line2\n")) != 0 {
		t.Fatalf("Expected \"line2\", got %q", string(line.Bytes))
	}
	if line.Position != 6 {
		t.Fatalf("Expected position=6, got %d", line.Position)
	}

	line, err = reader.Read()
	if err != io.EOF {
		t.Fatalf("Expected err = io.EOF, got %#v", err)
	}
}

func TestLineReaderReadingWindowsEndings(t *testing.T) {
}

func TestLineReaderReadingOpenFile(t *testing.T) {
}
