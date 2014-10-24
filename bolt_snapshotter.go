package main

import (
	"github.com/boltdb/bolt"
	"strconv"
)

const (
	boltSnapshotterBucket = "high_water_marks"
)

type BoltSnapshotter struct {
	DB *bolt.DB
}

func (s *BoltSnapshotter) HighWaterMark(filePath string) (*HighWaterMark, error) {
	highWaterMark := &HighWaterMark{FilePath: filePath}
	err := s.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltSnapshotterBucket))
		if bucket == nil {
			return nil
		}

		positionBytes := bucket.Get([]byte(filePath))
		if positionBytes == nil {
			return nil
		}

		position, err := strconv.ParseInt(string(positionBytes), 10, 64)
		if err != nil {
			return err
		}

		highWaterMark.Position = position
		return nil
	})

	if err != nil {
		return nil, err
	}
	return highWaterMark, nil
}

func (s *BoltSnapshotter) SetHighWaterMarks(marks []*HighWaterMark) error {
	err := s.DB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(boltSnapshotterBucket))
		if err != nil {
			return err
		}

		for _, mark := range marks {
			err = bucket.Put([]byte(mark.FilePath), []byte(strconv.FormatInt(mark.Position, 10)))
			if err != nil {
				return err
			}
		}

		return nil
	})

	return err
}
