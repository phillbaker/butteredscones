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
	clients     map[string]*ClientStatistics
	clientsLock sync.RWMutex

	files     map[string]*FileStatistics
	filesLock sync.RWMutex
}

const (
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

const (
	// The client is sending data
	clientStatusSending = "sending"

	// The client failed to send data and is waiting to retry
	clientStatusRetrying = "retrying"
)

type ClientStatistics struct {
	Status string `json:"status"`

	// The number of lines sent successfully to the client
	LinesSent int `json:"lines_sent"`

	// The last time lines were successfully sent to this client
	LastSendTime time.Time
}

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
		clients: make(map[string]*ClientStatistics),
		files:   make(map[string]*FileStatistics),
	}
}

func (s *Statistics) SetClientStatus(clientName string, status string) {
	s.ensureClientStatisticsCreated(clientName)

	stats := s.GetClientStatistics(clientName)
	stats.Status = status
}

func (s *Statistics) IncrementClientLinesSent(clientName string, linesSent int) {
	s.ensureClientStatisticsCreated(clientName)

	stats := s.GetClientStatistics(clientName)
	stats.LinesSent += linesSent
	stats.LastSendTime = time.Now()
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

func (s *Statistics) GetClientStatistics(clientName string) *ClientStatistics {
	s.filesLock.RLock()
	defer s.filesLock.RUnlock()

	return s.clients[clientName]
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

func (s *Statistics) ensureClientStatisticsCreated(clientName string) {
	// Fast check
	if _, ok := s.clients[clientName]; !ok {
		s.clientsLock.Lock()
		// Check again in the critical region
		if _, ok := s.clients[clientName]; !ok {
			s.clients[clientName] = &ClientStatistics{}
		}
		s.clientsLock.Unlock()
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
		"clients": s.clients,
		"files":   s.files,
	}

	return json.Marshal(structure)
}
