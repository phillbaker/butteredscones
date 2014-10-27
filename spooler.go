package main

import (
	"time"
)

// Spooler accepts input from the Input channel and creates currentChunks of Size,
// sending them to the Output channel. Timeout specifies the maximum duration
// items can be queued without a flush, regardless of Size.
type Spooler struct {
	Size    int
	Timeout time.Duration
}

func (s *Spooler) Spool(input chan *FileData, output chan []*FileData) {
	timer := time.NewTimer(s.Timeout)
	currentChunk := make([]*FileData, 0, s.Size)
	for {
		select {
		case fileData, ok := <-input:
			if ok {
				currentChunk = append(currentChunk, fileData)
				if len(currentChunk) >= s.Size {
					s.sendChunk(output, currentChunk)
					currentChunk = make([]*FileData, 0, s.Size)
				}
			} else {
				return
			}
		case <-timer.C:
			if len(currentChunk) > 0 {
				s.sendChunk(output, currentChunk)
				currentChunk = make([]*FileData, 0, s.Size)
			}
			timer.Reset(s.Timeout)
		}
	}
}

func (s *Spooler) sendChunk(output chan []*FileData, chunk []*FileData) {
	if len(chunk) > 0 {
		output <- chunk
	}
}
