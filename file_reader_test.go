package main

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestLineReaderReadingFileWithFields(t *testing.T) {
	file, err := os.Open("fixtures/basic.log")
	if err != nil {
		t.Fatal(err)
	}

	reader := &FileReader{
		File:   file,
		Fields: map[string]string{"type": "syslog"},
	}
	fileData, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if fileData.Data["line"] != "line1" {
		t.Fatalf("Expected \"line1\", got %q", fileData.Data["line"])
	}
	if fileData.Data["type"] != "syslog" {
		t.Fatalf("Expected \"type\":\"syslog\", got %q", fileData.Data["type"])
	}
	if fileData.HighWaterMark != 6 {
		t.Fatalf("Expected HighWaterMark=6, got %d", fileData.HighWaterMark)
	}

	fileData, err = reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if fileData.Data["line"] != "line2" {
		t.Fatalf("Expected \"line2\", got %q", fileData.Data["line"])
	}
	if fileData.Data["type"] != "syslog" {
		t.Fatalf("Expected \"type\":\"syslog\", got %q", fileData.Data["type"])
	}
	if fileData.HighWaterMark != 12 {
		t.Fatalf("Expected HighWaterMark=12, got %d", fileData.HighWaterMark)
	}

	fileData, err = reader.ReadLine()
	if err != io.EOF {
		t.Fatalf("Expected err = io.EOF, got %#v", err)
	}
}

func TestLineReaderReadingWindowsEndings(t *testing.T) {
	file, err := os.Open("fixtures/windows.log")
	if err != nil {
		t.Fatal(err)
	}

	reader := &FileReader{File: file}
	fileData, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if fileData.Data["line"] != "line1" {
		t.Fatalf("Expected \"line1\", got %q", fileData.Data["line"])
	}
	if fileData.HighWaterMark != 7 {
		t.Fatalf("Expected HighWaterMark=7, got %d", fileData.HighWaterMark)
	}

	fileData, err = reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if fileData.Data["line"] != "line2" {
		t.Fatalf("Expected \"line2\", got %q", fileData.Data["line"])
	}
	if fileData.HighWaterMark != 14 {
		t.Fatalf("Expected HighWaterMark=14, got %d", fileData.HighWaterMark)
	}

	fileData, err = reader.ReadLine()
	if err != io.EOF {
		t.Fatalf("Expected err = io.EOF, got %#v", err)
	}
}

func TestLineReaderReadingOpenFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "buttered-scones")
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	reader := &FileReader{File: file}

	tmpFile.Write([]byte("line1\n"))
	fileData, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if fileData.Data["line"] != "line1" {
		t.Fatalf("Expected \"line1\", got %q", fileData.Data["line"])
	}

	fileData, err = reader.ReadLine()
	if err != io.EOF {
		t.Fatalf("Expected err = io.EOF, got %#v", err)
	}
}

func TestLineReaderPartialLine(t *testing.T) {
	file, err := os.Open("fixtures/partial-line.log")
	if err != nil {
		t.Fatal(err)
	}

	reader := &FileReader{File: file}
	fileData, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if fileData.Data["line"] != "line1" {
		t.Fatalf("Expected \"line1\", got %q", fileData.Data["line"])
	}
	if fileData.HighWaterMark != 6 {
		t.Fatalf("Expected HighWaterMark=6, got %d", fileData.HighWaterMark)
	}

	fileData, err = reader.ReadLine()
	if fileData != nil {
		t.Fatalf("Expected fileData = nil after a partial line read, but got %#v", fileData)
	}
	if err != io.EOF {
		t.Fatalf("Expected err = io.EOF, got %#v", err)
	}
}
