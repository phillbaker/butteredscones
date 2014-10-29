package main

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Statistics keeps stats about the current operation of the program. It is
// meant to keep snapshot-in-time stats, as opposed to counters or timers that
// statsd offers.
//
// Statistics may be exposed by APIs that allow human- or machine-readable
// monitoring.
type Statistics struct {
	buffers *BufferStatistics

	files map[string]*FileStatistics

	// Synchronizes access to the Files map
	filesLock sync.RWMutex
}

type BufferStatistics struct {
	// Number of lines in the spooler "in" buffer that have yet to be spooled
	// into chunks that are ready to send.
	LinesBuffered int `json:"lines_buffered"`

	// Number of chunks in the spooler "out" buffer that are ready to be sent
	// to the remote server.
	ChunksBuffered int `json:"chunks_buttered"`

	// The last time data was successfully sent from the buffer to the remote
	// server.
	LastSendTime time.Time `json:"last_send_time"`
}

const (
	// The status of the file has not yet been explicitly set.
	fileStatusUnknown = "unknown"

	// The file is currently being read.
	fileStatusReading = "reading"

	// The file has been read to the end. In a few minutes, the file will be
	// closed. Or, if more data is written, the status will go back to reading.
	fileStatusEof = "eof"

	// The file is no longer being read. The file has been read to EOF and it
	// has not yet been reopened. If the file has been deleted, it will never
	// be reopened and will remain in this status until the process restarts.
	fileStatusClosed = "closed"
)

type FileStatistics struct {
	Status string `json:"status"`

	// The current size of the file.
	Size int64 `json:"size"`

	// The current position (in bytes) that has been read into the file. This
	// might be greater than SnapshotPosition if there are lines buffered into
	// memory that haven't been acknowledged by the server
	Position int64 `json:"position"`

	// The last time the file was read from into the in-memory buffer.
	LastRead time.Time `json:"last_read"`

	// The current position (in bytes) that has been successfully sent and
	// acknowledged by the remote server.
	SnapshotPosition int64 `json:"snapshot_position"`

	// The last time a line from this file was successfully sent and acknowledged
	// by the remote server.
	LastSnapshot time.Time `json:"last_snapshot"`
}

var GlobalStatistics *Statistics = NewStatistics()

func NewStatistics() *Statistics {
	return &Statistics{
		buffers: &BufferStatistics{},
		files:   make(map[string]*FileStatistics),
	}
}

func (s *Statistics) SetLinesBuffered(lines int) {
	s.buffers.LinesBuffered = lines
}

func (s *Statistics) SetChunksBuffered(chunks int) {
	s.buffers.ChunksBuffered = chunks
}

func (s *Statistics) SetLastSendTime(time time.Time) {
	s.buffers.LastSendTime = time
}

func (s *Statistics) SetFileStatus(filePath string, status string) {
	s.ensureFileStatisticsCreated(filePath)

	stats := s.GetFileStatistics(filePath)
	stats.Status = status
}

func (s *Statistics) SetFilePosition(filePath string, position int64) {
	s.ensureFileStatisticsCreated(filePath)

	stats := s.GetFileStatistics(filePath)
	stats.Position = position
	stats.LastRead = time.Now()
}

func (s *Statistics) SetFileSnapshotPosition(filePath string, snapshotPosition int64) {
	s.ensureFileStatisticsCreated(filePath)

	stats := s.GetFileStatistics(filePath)
	stats.SnapshotPosition = snapshotPosition
	stats.LastSnapshot = time.Now()
}

func (s *Statistics) GetFileStatistics(filePath string) *FileStatistics {
	s.filesLock.RLock()
	defer s.filesLock.RUnlock()

	return s.files[filePath]
}

// UpdateFileSizeStatistics updates the Size attribute of each file, so it's
// easier to compare how much progress buttered-scones has made through a file.
//
// UpdateFileSizeStatistics should be called before displaying statistics to
// an end user.
func (s *Statistics) UpdateFileSizeStatistics() {
	s.filesLock.RLock()
	filePaths := make([]string, 0, len(s.files))
	for filePath, _ := range s.files {
		filePaths = append(filePaths, filePath)
	}
	s.filesLock.RUnlock()

	for _, filePath := range filePaths {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			// unknown size; maybe it was deleted?
			s.files[filePath].Size = int64(-1)
		} else {
			s.files[filePath].Size = fileInfo.Size()
		}
	}
}

func (s *Statistics) ensureFileStatisticsCreated(filePath string) {
	// Fast check
	if _, ok := s.files[filePath]; !ok {
		s.filesLock.Lock()
		// Check again in the critical region
		if _, ok := s.files[filePath]; !ok {
			s.files[filePath] = &FileStatistics{}
		}
		s.filesLock.Unlock()
	}
}

func (s *Statistics) MarshalJSON() ([]byte, error) {
	structure := map[string]interface{}{
		"buffers": s.buffers,
		"files":   s.files,
	}

	return json.Marshal(structure)
}
