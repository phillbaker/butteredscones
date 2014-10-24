package main

import (
	"bufio"
	"bytes"
	"os"
)

type FileData struct {
	Data
	FilePath string

	// HighWaterMark is the index in the file after this line. Seeking to it
	// would read the next line.
	HighWaterMark int64
}

type FileReader struct {
	File *os.File

	// Fields are user-configurable keys/values that are merged into the Data
	// that is sent to the remote system.
	Fields map[string]string

	position int64
	buf      *bufio.Reader
}

func (h *FileReader) ReadLine() (*FileData, error) {
	if h.position == 0 {
		position, err := h.File.Seek(0, os.SEEK_CUR)
		if err != nil {
			return nil, err
		}

		h.position = position
	}

	if h.buf == nil {
		h.buf = bufio.NewReader(h.File)
	}

	line, err := h.buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	h.position += int64(len(line))

	fileData := &FileData{
		Data:          h.buildDataWithLine(bytes.TrimRight(line, "\r\n")),
		FilePath:      h.File.Name(),
		HighWaterMark: h.position,
	}
	return fileData, nil
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
