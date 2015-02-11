package butteredscones

type HighWaterMark struct {
	FilePath string

	// Position is the index in the file after a given line. Seeking to it would
	// read the next line.
	Position int64
}

type Snapshotter interface {
	HighWaterMark(filePath string) (*HighWaterMark, error)
	SetHighWaterMarks(marks []*HighWaterMark) error
}

type MemorySnapshotter struct {
	files map[string]int64
}

func (s *MemorySnapshotter) HighWaterMark(filePath string) (*HighWaterMark, error) {
	highWaterMark := &HighWaterMark{FilePath: filePath}
	if s.files != nil {
		highWaterMark.Position = s.files[filePath]
	}

	return highWaterMark, nil
}

func (s *MemorySnapshotter) SetHighWaterMarks(marks []*HighWaterMark) error {
	if s.files == nil {
		s.files = make(map[string]int64)
	}

	for _, mark := range marks {
		s.files[mark.FilePath] = mark.Position
	}
	return nil
}
