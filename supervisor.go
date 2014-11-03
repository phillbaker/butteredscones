package main

import (
	"github.com/technoweenie/grohl"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	supervisorReaderChunkSize = 64
)

type Supervisor struct {
	files       []FileConfiguration
	clients     []Client
	snapshotter Snapshotter

	// Optional settings
	SpoolSize int

	// How frequently to glob for new files that may have appeared
	GlobRefresh time.Duration
	globTimer   *time.Timer

	readerPool  *FileReaderPool
	readyChunks chan *readyChunk
	// A separate channel for retries to avoid deadlocking when multiple clients
	// need to retry.
	retryChunks chan *readyChunk

	stopRequest chan interface{}
	routineWg   sync.WaitGroup
}

type readyChunk struct {
	Chunk         []*FileData
	LockedReaders []*FileReader
}

func NewSupervisor(files []FileConfiguration, clients []Client, snapshotter Snapshotter) *Supervisor {
	spoolSize := 1024

	return &Supervisor{
		files:       files,
		clients:     clients,
		snapshotter: snapshotter,

		// Can be adjusted by clients later before calling Start
		SpoolSize:   spoolSize,
		GlobRefresh: 10 * time.Second,
	}
}

// Start pulls things together and plays match-maker.
func (s *Supervisor) Start() {
	s.stopRequest = make(chan interface{})

	s.readerPool = NewFileReaderPool()
	s.readyChunks = make(chan *readyChunk, len(s.clients))
	s.retryChunks = make(chan *readyChunk, len(s.clients))

	s.routineWg.Add(1)
	go func() {
		s.populateReaderPool()
		s.routineWg.Done()
	}()

	s.routineWg.Add(1)
	go func() {
		s.populateReadyChunks()
		s.routineWg.Done()
	}()

	for _, client := range s.clients {
		s.routineWg.Add(1)
		go func(client Client) {
			s.sendReadyChunksToClient(client)
			s.routineWg.Done()
		}(client)
	}
}

// Stop stops the supervisor cleanly, making sure all progress has been snapshotted
// before exiting.
func (s *Supervisor) Stop() {
	close(s.stopRequest)
	s.routineWg.Wait()
}

// Reads chunks from available file readers, putting together ready 'chunks'
// that can be sent to clients.
func (s *Supervisor) populateReadyChunks() {
	backoff := &ExponentialBackoff{Minimum: 50 * time.Millisecond, Maximum: 5000 * time.Millisecond}
	for {
		available, locked := s.readerPool.Counts()
		GlobalStatistics.UpdateFileReaderPoolStatistics(available, locked)

		currentChunk := &readyChunk{
			Chunk:         make([]*FileData, 0),
			LockedReaders: make([]*FileReader, 0),
		}

		for len(currentChunk.Chunk) < s.SpoolSize {
			if reader := s.readerPool.LockNext(); reader != nil {
				select {
				case <-s.stopRequest:
					return
				case chunk, ok := <-reader.C:
					if ok {
						currentChunk.Chunk = append(currentChunk.Chunk, chunk...)
						currentChunk.LockedReaders = append(currentChunk.LockedReaders, reader)

						if len(chunk) > 0 {
							if hwm := chunk[len(chunk)-1].HighWaterMark; hwm != nil {
								GlobalStatistics.SetFilePosition(hwm.FilePath, hwm.Position)
							}
						}
					} else {
						// The reader hit EOF or another error. Remove it and it'll get
						// picked up by populateReaderPool again if it still needs to be
						// read.
						s.readerPool.Remove(reader)
						GlobalStatistics.DeleteFileStatistics(reader.FilePath())
					}
				default:
					// The reader didn't have anything queued up for us. Unlock the
					// reader and move on.
					s.readerPool.Unlock(reader)
				}
			} else {
				// If there are no more readers, send the chunk ASAP so we can get
				// the next chunk in line
				grohl.Log(grohl.Data{"msg": "no readers available", "resolution": "sending current chunk"})
				break
			}
		}

		if len(currentChunk.Chunk) > 0 {
			select {
			case <-s.stopRequest:
				return
			case s.readyChunks <- currentChunk:
				backoff.Reset()
			}
		} else {
			select {
			case <-s.stopRequest:
				return
			case <-time.After(backoff.Next()):
				grohl.Log(grohl.Data{"msg": "no lines available to send", "resolution": "backing off"})
			}
		}
	}
}

// sendReadyChunksToClient reads from the readyChunks channel for a particular
// client, sending those chunks to the remote system. This function is also
// responsible for snapshotting progress and unlocking the readers after it has
// successfully sent.
func (s *Supervisor) sendReadyChunksToClient(client Client) {
	backoff := &ExponentialBackoff{Minimum: 50 * time.Millisecond, Maximum: 5000 * time.Millisecond}
	for {
		var readyChunk *readyChunk
		select {
		case <-s.stopRequest:
			return
		case readyChunk = <-s.retryChunks:
			// got a retry chunk; use it
		default:
			// pull from the default readyChunk queue
			select {
			case <-s.stopRequest:
				return
			case readyChunk = <-s.readyChunks:
				// got a chunk
			}
		}

		if readyChunk != nil {
			GlobalStatistics.SetClientStatus(client.Name(), clientStatusSending)
			if err := s.sendChunk(client, readyChunk.Chunk); err != nil {
				grohl.Report(err, grohl.Data{"msg": "failed to send chunk", "resolution": "retrying"})
				GlobalStatistics.SetClientStatus(client.Name(), clientStatusRetrying)

				// Put the chunk back on the queue for someone else to try
				select {
				case <-s.stopRequest:
					return
				case s.retryChunks <- readyChunk:
					// continue
				}

				// Backoff
				select {
				case <-s.stopRequest:
					return
				case <-time.After(backoff.Next()):
					// continue
				}
			} else {
				backoff.Reset()
				GlobalStatistics.IncrementClientLinesSent(client.Name(), len(readyChunk.Chunk))

				// Snapshot progress
				if err := s.acknowledgeChunk(readyChunk.Chunk); err != nil {
					grohl.Report(err, grohl.Data{"msg": "failed to acknowledge progress", "resolution": "skipping"})
				}

				s.readerPool.UnlockAll(readyChunk.LockedReaders)
			}
		}
	}
}

func (s *Supervisor) sendChunk(client Client, chunk []*FileData) error {
	lines := make([]Data, 0, len(chunk))
	for _, fileData := range chunk {
		lines = append(lines, fileData.Data)
	}

	return client.Send(lines)
}

func (s *Supervisor) acknowledgeChunk(chunk []*FileData) error {
	marks := make([]*HighWaterMark, 0, len(chunk))
	for _, fileData := range chunk {
		marks = append(marks, fileData.HighWaterMark)
	}

	err := s.snapshotter.SetHighWaterMarks(marks)
	if err == nil {
		// Update statistics
		for _, mark := range marks {
			GlobalStatistics.SetFileSnapshotPosition(mark.FilePath, mark.Position)
		}
	}

	return err
}

// populateReaderPool periodically globs for new files or files that previously
// hit EOF and creates file readers for them.
func (s *Supervisor) populateReaderPool() {
	logger := grohl.NewContext(grohl.Data{"ns": "Supervisor", "fn": "populateReaderPool"})

	timer := time.NewTimer(0)
	for {
		select {
		case <-s.stopRequest:
			return
		case <-timer.C:
			logTimer := logger.Timer(grohl.Data{})
			for _, config := range s.files {
				for _, path := range config.Paths {
					matches, err := filepath.Glob(path)
					if err != nil {
						logger.Report(err, grohl.Data{"path": path, "msg": "failed to glob", "resolution": "skipping path"})
						continue
					}

					for _, filePath := range matches {
						if err = s.startFileReader(filePath, config.Fields); err != nil {
							logger.Report(err, grohl.Data{"path": path, "filePath": filePath, "msg": "failed to start reader", "resolution": "skipping file"})
						}
					}
				}
			}
			logTimer.Finish()
			timer.Reset(s.GlobRefresh)
		}
	}
}

// startFileReader starts an individual file reader at a given path, if one
// isn't already running.
func (s *Supervisor) startFileReader(filePath string, fields map[string]string) error {
	// There's already a reader in the pool for this path
	if s.readerPool.IsPathInPool(filePath) {
		return nil
	}

	highWaterMark, err := s.snapshotter.HighWaterMark(filePath)
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	_, err = file.Seek(highWaterMark.Position, os.SEEK_SET)
	if err != nil {
		file.Close()
		return err
	}

	reader, err := NewFileReader(file, fields, supervisorReaderChunkSize)
	if err != nil {
		file.Close()
		return err
	}

	s.readerPool.Add(reader)
	return nil
}
