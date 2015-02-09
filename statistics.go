package butteredscones

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

	fileReaderPool *FileReaderPoolStatistics

	files     map[string]*FileStatistics
	filesLock sync.RWMutex
}

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
	LastSendTime time.Time `json:"last_send_time"`

	// The number of lines in the last chunk successfully sent to this client
	LastChunkSize int `json:"last_chunk_size"`
}

type FileReaderPoolStatistics struct {
	// The number of files in the pool that are available to be read
	Available int `json:"available"`

	// The number of files in the pool that are locked, ready to be sent, but
	// haven't been yet.
	Locked int `json:"locked"`
}

type FileStatistics struct {
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
		clients:        make(map[string]*ClientStatistics),
		fileReaderPool: &FileReaderPoolStatistics{},
		files:          make(map[string]*FileStatistics),
	}
}

func (s *Statistics) SetClientStatus(clientName string, status string) {
	s.filesLock.Lock()
	defer s.filesLock.Unlock()

	stats := s.ensureClientStatisticsCreated(clientName)
	stats.Status = status
}

func (s *Statistics) IncrementClientLinesSent(clientName string, linesSent int) {
	s.filesLock.Lock()
	defer s.filesLock.Unlock()

	stats := s.ensureClientStatisticsCreated(clientName)
	stats.LastChunkSize = linesSent
	stats.LinesSent += linesSent
	stats.LastSendTime = time.Now()
}

func (s *Statistics) UpdateFileReaderPoolStatistics(available int, locked int) {
	s.fileReaderPool.Available = available
	s.fileReaderPool.Locked = locked
}

func (s *Statistics) SetFilePosition(filePath string, position int64) {
	s.filesLock.Lock()
	defer s.filesLock.Unlock()

	stats := s.ensureFileStatisticsCreated(filePath)
	stats.Position = position
	stats.LastRead = time.Now()
}

func (s *Statistics) SetFileSnapshotPosition(filePath string, snapshotPosition int64) {
	s.filesLock.Lock()
	defer s.filesLock.Unlock()

	stats := s.ensureFileStatisticsCreated(filePath)
	stats.SnapshotPosition = snapshotPosition
	stats.LastSnapshot = time.Now()
}

func (s *Statistics) DeleteFileStatistics(filePath string) {
	s.filesLock.Lock()
	defer s.filesLock.Unlock()

	delete(s.files, filePath)
}

// UpdateFileSizeStatistics updates the Size attribute of each file, so it's
// easier to compare how much progress butteredscones has made through a file.
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
		if stats := s.files[filePath]; stats != nil {
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				// unknown size; maybe it was deleted?
				stats.Size = int64(-1)
			} else {
				stats.Size = fileInfo.Size()
			}
		}
	}
}

func (s *Statistics) ensureClientStatisticsCreated(clientName string) *ClientStatistics {
	// assumes lock is held by the caller
	if _, ok := s.clients[clientName]; !ok {
		s.clients[clientName] = &ClientStatistics{}
	}

	return s.clients[clientName]
}

func (s *Statistics) ensureFileStatisticsCreated(filePath string) *FileStatistics {
	// assumes lock is held by the caller
	if _, ok := s.files[filePath]; !ok {
		s.files[filePath] = &FileStatistics{}
	}

	return s.files[filePath]
}

func (s *Statistics) MarshalJSON() ([]byte, error) {
	structure := map[string]interface{}{
		"clients":          s.clients,
		"file_reader_pool": s.fileReaderPool,
		"files":            s.files,
	}

	return json.Marshal(structure)
}
