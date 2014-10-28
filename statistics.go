package main

import (
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
	files map[string]*FileStatistics

	// Synchronizes access to the Files map
	filesLock sync.RWMutex
}

const (
	// The status of the file has not yet been explicitly set.
	fileStatusUnknown = iota

	// The file is currently being read.
	fileStatusReading = iota

	// The file has been read to the end. In a few minutes, the file will be
	// closed. Or, if more data is written, the status will go back to reading.
	fileStatusEof = iota

	// The file is no longer being read. The file has been read to EOF and it
	// has not yet been reopened. If the file has been deleted, it will never
	// be reopened and will remain in this status until the process restarts.
	fileStatusClosed = iota
)

type FileStatistics struct {
	Status int

	// The current position (in bytes) that has been read into the file. This
	// might be greater than SnapshotPosition if there are lines buffered into
	// memory that haven't been acknowledged by the server
	Position int64

	// The last time the file was read from into the in-memory buffer.
	LastRead time.Time

	// The current position (in bytes) that has been successfully sent and
	// acknowledged by the remote server.
	SnapshotPosition int64

	// The last time a line from this file was successfully sent and acknowledged
	// by the remote server.
	LastSnapshot time.Time
}

var GlobalStatistics *Statistics = NewStatistics()

func NewStatistics() *Statistics {
	return &Statistics{}
}

func (s *Statistics) SetFileStatus(filePath string, status int) {
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

func (s *Statistics) ensureFileStatisticsCreated(filePath string) {
	_, ok := s.files[filePath]
	if !ok {
		s.filesLock.Lock()
		s.files[filePath] = &FileStatistics{}
		s.filesLock.Unlock()
	}
}
