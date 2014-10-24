package main

import (
	"bufio"
	"io"
)

type LineReader struct {
	buf      *bufio.Reader
	position int
}

type Line struct {
	// Bytes in the line, including any line ending characters.
	Bytes []byte

	// The position where this line started. `Position + len(Bytes)` represents
	// the position where the reader will read from next.
	Position int
}

func NewLineReader(io io.Reader, initialPosition int) *LineReader {
	return &LineReader{
		buf:      bufio.NewReader(io),
		position: initialPosition,
	}
}

// Read reads a line from the file and updates its internal position cursor.
// If Read returns an error, consumers should assume that the Reader is no
// longer valid and should discard it, recreating it if they want to read the
// same file again.
func (e *LineReader) Read() (*Line, error) {
	bytes, err := e.buf.ReadBytes('\n')
	if err != nil {
		// Internal position intentionally not updated, even if a partial line is
		// read. This keeps complexity lower: if a partial line was read, consumers
		// should simply reopen the file and recreate the LineReader, seeking to
		// the previous position to see if a full line was eventually written.
		return nil, err
	}

	line := &Line{
		Bytes:    bytes,
		Position: e.position,
	}
	e.position += len(bytes)

	return line, nil
}

func (e *LineReader) Position() int {
	return e.position
}
