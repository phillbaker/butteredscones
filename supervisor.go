package main

import (
	"github.com/technoweenie/grohl"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Supervisor struct {
	Files []FileConfiguration
	Client
	Snapshotter
	SpoolSize    int
	SpoolTimeout time.Duration

	// How frequently to glob for new files that may have appeared
	GlobRefresh time.Duration
}

const (
	// The number of 'chunks' ready to be sent to the remote server to keep in
	// memory.
	supervisorSpoolOutSize = 16
)

// Pulls the entire program together. Connects file readers to a spooler to
// a client, snapshotting progress after a successful acknowledgement from
// the server.
//
// To stop the supervisor, send a message to the done channel.
func (s *Supervisor) Serve(done chan interface{}) {
	logger := grohl.NewContext(grohl.Data{"ns": "Supervisor"})

	spooler := &Spooler{
		Size:    s.SpoolSize,
		Timeout: s.SpoolTimeout,
	}
	spoolIn := make(chan *FileData, s.SpoolSize*10)
	spoolOut := make(chan []*FileData, supervisorSpoolOutSize)
	go spooler.Spool(spoolIn, spoolOut)
	defer func() { close(spoolIn) }()

	readers := new(FileReaderCollection)
	s.startFileReaders(spoolIn, readers)

	// In the case a chunk fails to send correctly, the chunk is added to retryCh
	// which is read from at a priority over spoolOut. Only one chunk may be
	// added to retryCh.
	retryCh := make(chan []*FileData, 1)

	globTicker := time.NewTicker(s.GlobRefresh)
	for {
		var chunkToSend []*FileData
		select {
		case <-done:
			return
		case chunkToSend = <-retryCh:
			// Retrying a previous chunk
		default:
			// We are were not terminated and no "retry chunk" took priority. Let's
			// check the other queues.
			select {
			case chunkToSend = <-spoolOut:
				// Attempting to send a new chunk
			case <-globTicker.C:
				// FUTURE:  Globbing could be done in a separate goroutine, provided it
				// had its own critical region
				s.startFileReaders(spoolIn, readers)
			}
		}

		if chunkToSend != nil {
			err := s.sendChunk(chunkToSend)
			if err != nil {
				logger.Report(err, grohl.Data{"msg": "failed to send chunk", "resolution": "retrying"})
				retryCh <- chunkToSend
			} else {
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

func (s *Supervisor) sendChunk(chunk []*FileData) error {
	lines := make([]Data, 0, len(chunk))
	for _, fileData := range chunk {
		lines = append(lines, fileData.Data)
	}

	return s.Client.Send(lines)
}

func (s *Supervisor) acknowledgeChunk(chunk []*FileData) error {
	marks := make([]*HighWaterMark, 0, len(chunk))
	for _, fileData := range chunk {
		marks = append(marks, fileData.HighWaterMark)
	}

	return s.Snapshotter.SetHighWaterMarks(marks)
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

// startFileReader start an individual file reader at a given path, if one
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
		s.runFileReader(spoolIn, reader)

		// When the reader is deleted from the collection, it's eligible to be
		// recreated when glob runs again.
		readers.Delete(filePath)
	}()

	return nil
}

// runFileReader reads from a FileReader until EOF is reached
func (s *Supervisor) runFileReader(spoolIn chan *FileData, reader *FileReader) {
	logger := grohl.NewContext(grohl.Data{"ns": "Supervisor", "fn": "runFileReader", "file": reader.File.Name()})

	// Track the "last position" that has been sent to the spool channel. If we
	// encounter an error, we want to make sure that position has been
	// snapshotted before we exit. Otherwise, a new file reader might be created
	// and repeat log lines.
	lastPosition := reader.Position()
	for {
		fileData, err := reader.ReadLine()
		if err == io.EOF {
			logger.Log(grohl.Data{"status": "EOF", "resolution": "closing file"})
			break
		} else if err != nil {
			logger.Report(err, grohl.Data{"msg": "failed to completely read file", "resolution": "closing file"})
			break
		}

		spoolIn <- fileData
		lastPosition = reader.Position()
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
