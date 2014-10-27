package main

import (
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

	globTicker := time.NewTicker(s.GlobRefresh)
	for {
		select {
		case <-done:
			return

		case chunk := <-spoolOut:
			// FUTURE: Sending and acknowledging could be done in a separate
			// goroutine, provided it had its own critical region
			err := s.sendAndAcknowledge(chunk)
			if err != nil {
				// LOG
			}

		case <-globTicker.C:
			// FUTURE:  Globbing could be done in a separate goroutine, provided it
			// had its own critical region
			s.startFileReaders(spoolIn, readers)
		}
	}
}

func (s *Supervisor) sendAndAcknowledge(chunk []*FileData) error {
	lines := make([]Data, 0, len(chunk))
	marks := make([]*HighWaterMark, 0, len(chunk))
	for _, fileData := range chunk {
		lines = append(lines, fileData.Data)
		marks = append(marks, fileData.HighWaterMark)
	}

	err := s.Client.Send(lines)
	if err != nil {
		return err
	}

	err = s.Snapshotter.SetHighWaterMarks(marks)
	if err != nil {
		return err
	}

	return nil
}

// startFileReaders globs the paths in each FileConfiguration, making sure
// a FileReader has been started for each one.
func (s *Supervisor) startFileReaders(spoolIn chan *FileData, readers *FileReaderCollection) {
	for _, config := range s.Files {
		for _, path := range config.Paths {
			matches, err := filepath.Glob(path)
			if err != nil {
				// LOG
				continue
			}

			for _, match := range matches {
				err = s.startFileReader(spoolIn, readers, match, config.Fields)
				if err != nil {
					// LOG
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
		defer readers.Delete(filePath)
		for {
			fileData, err := reader.ReadLine()
			if err == io.EOF {
				// Done
				return
			} else if err != nil {
				// LOG
				return
			}
			spoolIn <- fileData
		}
	}()

	return nil
}
