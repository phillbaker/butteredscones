package butteredscones

import (
	"sync"
)

type FileReaderPool struct {
	available map[string]*FileReader
	locked    map[string]*FileReader
	lock      sync.RWMutex
}

func NewFileReaderPool() *FileReaderPool {
	return &FileReaderPool{
		available: make(map[string]*FileReader),
		locked:    make(map[string]*FileReader),
	}
}

func (p *FileReaderPool) Counts() (available int, locked int) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return len(p.available), len(p.locked)
}

// TODO: Figure out how to make this block, rather than return nil
func (p *FileReaderPool) LockNext() *FileReader {
	p.lock.Lock()
	defer p.lock.Unlock()

	for filePath, reader := range p.available {
		delete(p.available, filePath)
		p.locked[filePath] = reader
		return reader
	}

	// Nothing available to lock
	return nil
}

func (p *FileReaderPool) Unlock(reader *FileReader) {
	p.lock.Lock()
	defer p.lock.Unlock()

	filePath := reader.FilePath()
	delete(p.locked, filePath)
	p.available[filePath] = reader
}

func (p *FileReaderPool) UnlockAll(readers []*FileReader) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, reader := range readers {
		filePath := reader.FilePath()
		delete(p.locked, filePath)
		p.available[filePath] = reader
	}
}

func (p *FileReaderPool) IsPathInPool(filePath string) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return (p.available[filePath] != nil || p.locked[filePath] != nil)
}

func (p *FileReaderPool) Add(reader *FileReader) {
	p.lock.Lock()
	defer p.lock.Unlock()

	filePath := reader.FilePath()
	p.available[filePath] = reader
}

func (p *FileReaderPool) Remove(reader *FileReader) {
	p.lock.Lock()
	defer p.lock.Unlock()

	filePath := reader.FilePath()
	delete(p.available, filePath)
	delete(p.locked, filePath)
}
