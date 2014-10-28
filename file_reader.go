package main

import (
	"bufio"
	"bytes"
	"os"
)

type FileData struct {
	Data
	*HighWaterMark
}

type FileReader struct {
	File *os.File

	// Fields are user-configurable keys/values that are merged into the Data
	// that is sent to the remote system.
	Fields map[string]string

	position int64
	buf      *bufio.Reader

	partialBuf bytes.Buffer
}

func (h *FileReader) ReadLine() (*FileData, error) {
	h.initializePosition()

	if h.buf == nil {
		h.buf = bufio.NewReader(h.File)
	}

	line, err := h.buf.ReadBytes('\n')
	// It's possible to get both a partial line and an error (e.g., EOF). If that
	// happens, the partial line is written to a buffer and reused on the next
	// call.
	if line != nil {
		h.position += int64(len(line))
	}

	if err != nil {
		if line != nil {
			h.partialBuf.Write(line)
		}

		return nil, err
	} else {
		// If there is a partial buffer, use it
		if h.partialBuf.Len() > 0 {
			h.partialBuf.Write(line)
			line, _ = h.partialBuf.ReadBytes('\n')
		}

		fileData := &FileData{
			Data: h.buildDataWithLine(bytes.TrimRight(line, "\r\n")),
			HighWaterMark: &HighWaterMark{
				FilePath: h.File.Name(),
				Position: h.position,
			},
		}

		return fileData, nil
	}
}

func (h *FileReader) Position() int64 {
	h.initializePosition()

	return h.position
}

func (h *FileReader) initializePosition() error {
	if h.position == 0 {
		h.position, _ = h.File.Seek(0, os.SEEK_CUR)
	}

	return nil
}

func (h *FileReader) buildDataWithLine(line []byte) Data {
	var data Data
	if h.Fields != nil {
		data = make(Data, len(h.Fields)+1)
	} else {
		data = make(Data, 1)
	}
	data["line"] = string(line)

	for k, v := range h.Fields {
		data[k] = v
	}

	return data
}
