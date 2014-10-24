package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestLineEmitterReadingFile(t *testing.T) {
	file, err := os.Open("fixtures/basic.log")
	if err != nil {
		t.Fatal(err)
	}

	emitter := NewLineEmitter(file, 0)
	line, err := emitter.Emit()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(line.Bytes, []byte("line1\n")) != 0 {
		t.Fatalf("Expected \"line1\", got %q", string(line.Bytes))
	}
	if line.Position != 0 {
		t.Fatalf("Expected position=0, got %d", line.Position)
	}

	line, err = emitter.Emit()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(line.Bytes, []byte("line2\n")) != 0 {
		t.Fatalf("Expected \"line2\", got %q", string(line.Bytes))
	}
	if line.Position != 6 {
		t.Fatalf("Expected position=6, got %d", line.Position)
	}

	line, err = emitter.Emit()
	if err != io.EOF {
		t.Fatalf("Expected err = io.EOF, got %#v", err)
	}
}

func TestLineEmitterReadingWindowsEndings(t *testing.T) {
}

func TestLineEmitterReadingOpenFile(t *testing.T) {
}
