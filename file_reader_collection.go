package main

import (
	"sync"
)

// FileReaderCollection is a thread-safe mapping between a file path and a
// file reader. It is used to keep track of which files are currently being
// read.
type FileReaderCollection struct {
	readers map[string]*FileReader
	lock    sync.RWMutex
}

func (c *FileReaderCollection) Get(filePath string) *FileReader {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.readers != nil {
		return c.readers[filePath]
	} else {
		return nil
	}
}

func (c *FileReaderCollection) Delete(filePath string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.readers != nil {
		delete(c.readers, filePath)
	}
}

func (c *FileReaderCollection) Set(filePath string, reader *FileReader) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.readers == nil {
		c.readers = make(map[string]*FileReader)
	}
	c.readers[filePath] = reader
}
