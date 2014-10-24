package main

import (
	"io"
	"os"
	"time"
)

type Supervisor struct {
	Files []FileConfiguration
	Client
	Snapshotter
	*Spooler

	spoolIn  chan *FileData
	spoolOut chan []*FileData

	readers *FileReaderCollection
}

func (s *Supervisor) Serve() {
	s.spoolIn = make(chan *FileData, 1024)
	s.spoolOut = make(chan []*FileData, 128)
	go s.Spooler.Spool(s.spoolIn, s.spoolOut)

	s.readers = new(FileReaderCollection)

	err := s.startFileReader("fixtures/basic.log", map[string]string{})
	if err != nil {
		panic(err)
	}

	globTicker := time.NewTicker(1 * time.Minute)
	for {
		select {
		case chunk := <-s.spoolOut:
			err := s.sendAndAcknowledge(chunk)
			if err != nil {
				// LOG
			}

		case <-globTicker.C:
			// TODO: Glob dem new files
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

func (s *Supervisor) startFileReader(filePath string, fields map[string]string) error {
	if s.readers.Get(filePath) != nil {
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
	s.readers.Set(filePath, reader)
	go func() {
		defer s.readers.Delete(filePath)
		for {
			fileData, err := reader.ReadLine()
			if err == io.EOF {
				// Done
				return
			} else if err != nil {
				// LOG
				return
			}
			s.spoolIn <- fileData
		}
	}()

	return nil
}
