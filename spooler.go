package main

import (
	"time"
)

// Spooler accepts items on the In channel and chunks them into items on the
// Out channel.
type Spooler struct {
	In  chan *FileData
	Out chan []*FileData

	size    int
	timeout time.Duration
}

const (
	// The number of items that can be buffered in the Out channel.
	spoolOutBuffer = 16
)

func NewSpooler(size int, timeout time.Duration) *Spooler {
	return &Spooler{
		In:      make(chan *FileData, size*10),
		Out:     make(chan []*FileData, spoolOutBuffer),
		size:    size,
		timeout: timeout,
	}
}

// Spool accepts items from the In channel and spools them into the Out
// channel. To stop the spooling, close the In channel.
func (s *Spooler) Spool() {
	timer := time.NewTimer(s.timeout)
	currentChunk := make([]*FileData, 0, s.size)
	for {
		select {
		case fileData, ok := <-s.In:
			if ok {
				currentChunk = append(currentChunk, fileData)
				if len(currentChunk) >= s.size {
					s.sendChunk(currentChunk)
					currentChunk = make([]*FileData, 0, s.size)
				}
			} else {
				return
			}
		case <-timer.C:
			if len(currentChunk) > 0 {
				s.sendChunk(currentChunk)
				currentChunk = make([]*FileData, 0, s.size)
			}
		}

		timer.Reset(s.timeout)
	}
}

func (s *Spooler) sendChunk(chunk []*FileData) {
	if len(chunk) > 0 {
		s.Out <- chunk
	}
}
