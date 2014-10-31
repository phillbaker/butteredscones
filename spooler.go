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
	spoolOutBuffer = 4
)

func NewSpooler(size int, timeout time.Duration) *Spooler {
	return &Spooler{
		In:      make(chan *FileData, size*spoolOutBuffer),
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
					s.Out <- currentChunk
					currentChunk = make([]*FileData, 0, s.size)
				}
			} else {
				return
			}
		case <-timer.C:
			if len(currentChunk) > 0 {
				select {
				case s.Out <- currentChunk:
					currentChunk = make([]*FileData, 0, s.size)
				default:
					// Never block trying to send to the channel because of a timer
					// firing.  Otherwise, small chunks may be added to the channel. If
					// we can't send immediately, we might as well keep spooling to build
					// up a bigger chunk.
				}
			}
		}

		timer.Reset(s.timeout)
	}
}
