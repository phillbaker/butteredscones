package main

import (
	"bufio"
	"bytes"
	"github.com/technoweenie/grohl"
	"io"
	"os"
)

type FileData struct {
	Data
	*HighWaterMark
}

type FileReader struct {
	C         chan []*FileData
	ChunkSize int

	file     *os.File
	filePath string
	fields   map[string]string

	position int64
	buf      *bufio.Reader

	hostname string
}

func NewFileReader(file *os.File, fields map[string]string, chunkSize int) (*FileReader, error) {
	position, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil, err
	}

	hostname, _ := os.Hostname()

	reader := &FileReader{
		C:         make(chan []*FileData, 1),
		ChunkSize: chunkSize,
		file:      file,
		filePath:  file.Name(),
		fields:    fields,
		position:  position,
		buf:       bufio.NewReader(file),
		hostname:  hostname,
	}
	go reader.read()

	return reader, nil
}

func (h *FileReader) read() {
	logger := grohl.NewContext(grohl.Data{"ns": "FileReader", "file_path": h.filePath})

	currentChunk := make([]*FileData, 0, h.ChunkSize)
	for {
		line, err := h.buf.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				logger.Report(err, grohl.Data{"msg": "error reading file", "resolution": "closing file"})
			}

			h.sendChunk(currentChunk)
			close(h.C)

			return
		}
		h.position += int64(len(line))

		fileData := &FileData{
			Data: h.buildDataWithLine(bytes.TrimRight(line, "\r\n")),
			HighWaterMark: &HighWaterMark{
				FilePath: h.filePath,
				Position: h.position,
			},
		}
		currentChunk = append(currentChunk, fileData)

		if len(currentChunk) >= h.ChunkSize {
			h.sendChunk(currentChunk)
			currentChunk = make([]*FileData, 0, h.ChunkSize)
		}
	}
}

func (h *FileReader) FilePath() string {
	return h.filePath
}

func (h *FileReader) sendChunk(chunk []*FileData) {
	if len(chunk) > 0 {
		h.C <- chunk
	}
}

func (h *FileReader) buildDataWithLine(line []byte) Data {
	var data Data
	if h.fields != nil {
		data = make(Data, len(h.fields)+1)
	} else {
		data = make(Data, 2)
	}
	data["line"] = string(line)
	data["host"] = h.hostname

	for k, v := range h.fields {
		data[k] = v
	}

	return data
}
