package main

import (
	"container/ring"
	"github.com/technoweenie/grohl"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Supervisor struct {
	Files   []FileConfiguration
	Clients []Client
	Snapshotter
	SpoolSize    int
	SpoolTimeout time.Duration

	// How frequently to glob for new files that may have appeared
	GlobRefresh time.Duration
}

const (
	supervisorClientRetryMinimum = 500 * time.Millisecond
	supervisorClientRetryMaximum = 5 * time.Second

	supervisorEOFRetryMinimum = 50 * time.Millisecond
	supervisorEOFRetryMaximum = 5 * time.Second

	supervisorEOFTimeout = 5 * time.Minute
)

// Pulls the entire program together. Connects file readers to a spooler to
// a client, snapshotting progress after a successful acknowledgement from
// the server.
//
// To stop the supervisor, send a message to the done channel.
func (s *Supervisor) Serve(done chan interface{}) {
	logger := grohl.NewContext(grohl.Data{"ns": "Supervisor"})

	spooler := NewSpooler(s.SpoolSize, s.SpoolTimeout)
	go spooler.Spool()

	// Create a ring of clients so we alternate which client we use every time
	// we send.
	clientRing := ring.New(len(s.Clients))
	for _, client := range s.Clients {
		clientRing.Value = client
		clientRing = clientRing.Next()
	}

	readers := new(FileReaderCollection)
	s.startFileReaders(spooler.In, readers)

	// In the case that a chunk fails, we retry it by setting it as the
	// retryChunk.  We keep retrying the chunk until it sends correctly, then
	// move on to the normal queues.
	var retryChunk []*FileData
	var retryTimer *time.Timer = time.NewTimer(0)
	var retryBackoff *ExponentialBackoff = &ExponentialBackoff{
		Minimum: supervisorClientRetryMinimum,
		Maximum: supervisorClientRetryMaximum,
	}

	globTicker := time.NewTicker(s.GlobRefresh)
	for {
		var chunkToSend []*FileData
		if retryChunk != nil {
			// Retry case: after the retry timer, select retryChunk as the chunk to
			// send. Also monitor the other channels so we can do work in the
			// background if needed.
			select {
			case <-done:
				return
			case <-retryTimer.C:
				chunkToSend = retryChunk
			case <-globTicker.C:
				s.startFileReaders(spooler.In, readers)
			}
		} else {
			select {
			case <-done:
				return
			case chunkToSend = <-spooler.Out:
				// got a chunk; we'll send it below
			case <-globTicker.C:
				s.startFileReaders(spooler.In, readers)
			}
		}

		GlobalStatistics.SetLinesBuffered(len(spooler.In))
		GlobalStatistics.SetChunksBuffered(len(spooler.Out))

		if chunkToSend != nil {
			client := clientRing.Value.(Client)
			clientRing = clientRing.Next()

			sendStartTime := time.Now()
			err := s.sendChunk(client, chunkToSend)
			if err != nil {
				logger.Report(err, grohl.Data{"msg": "failed to send chunk", "resolution": "retrying"})

				retryChunk = chunkToSend
				retryTimer.Reset(retryBackoff.Next())
			} else {
				duration := time.Since(sendStartTime).Seconds()
				logger.Log(grohl.Data{
					"status":       "sent chunk",
					"chunk_size":   len(chunkToSend),
					"duration":     duration,
					"msgs_per_sec": float64(len(chunkToSend)) / duration,
				})

				retryChunk = nil
				retryTimer.Stop()
				retryBackoff.Reset()

				GlobalStatistics.SetLastSendTime(time.Now())

				err = s.acknowledgeChunk(chunkToSend)
				if err != nil {
					// This is trickier; we've already sent the chunk to the remote system
					// successfully; retrying it would just create duplicates. The best
					// thing we can do is report the error and assume it's transient ...
					// that the next time we acknowledge a chunk, the snapshot will
					// succeed.
					logger.Report(err, grohl.Data{"msg": "failed to snapshot high water marks"})
				}
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

	err := s.Snapshotter.SetHighWaterMarks(marks)
	if err == nil {
		// Update statistics
		for _, mark := range marks {
			GlobalStatistics.SetFileSnapshotPosition(mark.FilePath, mark.Position)
		}
	}

	return err
}

// startFileReaders globs the paths in each FileConfiguration, making sure
// a FileReader has been started for each one.
func (s *Supervisor) startFileReaders(spoolIn chan *FileData, readers *FileReaderCollection) {
	logger := grohl.NewContext(grohl.Data{"ns": "Supervisor", "fn": "startFileReaders"})

	for _, config := range s.Files {
		for _, path := range config.Paths {
			matches, err := filepath.Glob(path)
			if err != nil {
				logger.Report(err, grohl.Data{"path": path, "msg": "failed to glob", "resolution": "skipping path"})
				continue
			}

			for _, match := range matches {
				err = s.startFileReader(spoolIn, readers, match, config.Fields)
				if err != nil {
					logger.Report(err, grohl.Data{"path": "path", "match": match, "msg": "failed to start reader", "resolution": "skipping file"})
				}
			}
		}
	}
}

// startFileReader starts an individual file reader at a given path, if one
// isn't already running.
func (s *Supervisor) startFileReader(spoolIn chan *FileData, readers *FileReaderCollection, filePath string, fields map[string]string) error {
	if readers.Get(filePath) != nil {
		// There's already a reader for this file path. No need to do anything
		// further.
		return nil
	}

	highWaterMark, err := s.Snapshotter.HighWaterMark(filePath)
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

	reader := &FileReader{File: file, Fields: fields}
	readers.Set(filePath, reader)
	go func() {
		GlobalStatistics.SetFileStatus(filePath, fileStatusReading)
		GlobalStatistics.SetFilePosition(filePath, highWaterMark.Position)
		GlobalStatistics.SetFileSnapshotPosition(filePath, highWaterMark.Position)

		s.runFileReader(spoolIn, reader)

		// When the reader is deleted from the collection, it's eligible to be
		// recreated when glob runs again.
		GlobalStatistics.SetFileStatus(filePath, fileStatusClosed)
		readers.Delete(filePath)
	}()

	return nil
}

// runFileReader reads from a FileReader until EOF is reached
func (s *Supervisor) runFileReader(spoolIn chan *FileData, reader *FileReader) {
	logger := grohl.NewContext(grohl.Data{"ns": "Supervisor", "fn": "runFileReader", "file": reader.File.Name()})
	logger.Log(grohl.Data{"status": "opened"})

	// Track the "last position" that has been sent to the spool channel. If we
	// encounter an error, we want to make sure that position has been
	// snapshotted before we exit. Otherwise, a new file reader might be created
	// and repeat log lines.
	lastPosition := reader.Position()

	// Records the last time we receive an EOF; if we keep receiving an EOF,
	// we'll eventually exit.
	lastEof := time.Time{}

	// If we hit EOF, we exponentially backoff
	eofBackoff := &ExponentialBackoff{Minimum: supervisorEOFRetryMinimum, Maximum: supervisorEOFRetryMaximum}

	for {
		fileData, err := reader.ReadLine()
		if err == io.EOF {
			GlobalStatistics.SetFileStatus(reader.File.Name(), fileStatusEof)

			if lastEof.IsZero() {
				// Our first EOF: record it
				lastEof = time.Now()
			} else if time.Since(lastEof) >= supervisorEOFTimeout {
				logger.Log(grohl.Data{"status": "EOF", "resolution": "closing file"})
				break
			} else {
				<-time.After(eofBackoff.Next())
			}
		} else if err != nil {
			logger.Report(err, grohl.Data{"msg": "failed to completely read file", "resolution": "closing file"})
			break
		} else {
			lastEof = time.Time{}
			eofBackoff.Reset()

			GlobalStatistics.SetFileStatus(reader.File.Name(), fileStatusReading)
			GlobalStatistics.SetFilePosition(reader.File.Name(), reader.Position())

			spoolIn <- fileData
			lastPosition = reader.Position()
		}
	}

	// Wait until our last position has been snapshotted
	for {
		highWaterMark, err := s.Snapshotter.HighWaterMark(reader.File.Name())
		if err != nil {
			logger.Report(err, grohl.Data{"msg": "failed to read high water mark", "resolution": "retrying"})
		} else if highWaterMark.Position >= lastPosition {
			// Done! We can exit cleanly now.
			break
		}

		// Try again in a second
		<-time.After(1 * time.Second)
	}
}
