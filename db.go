package slowbolt

import (
	"os"
	"runtime"
	"time"

	"go.etcd.io/bbolt"
)

type (
	Tx          = bbolt.Tx
	TxStats     = bbolt.TxStats
	Bucket      = bbolt.Bucket
	BucketStats = bbolt.BucketStats
	Options     = bbolt.Options
)

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	db, err := bbolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db, SlowDuration: time.Minute}, nil
}

type DB struct {
	*bbolt.DB
	OnSlow       func(op, fn, file string, line int)
	SlowDuration time.Duration
}

func (b *DB) Update(fn func(*Tx) error) error {
	if b.SlowDuration == -1 {
		return b.DB.Update(fn)
	}
	var pcs [2]uintptr

	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	start := time.Now()

	err := b.DB.Update(fn)
	if took := time.Since(start); took > b.SlowDuration {
		var (
			fn   string
			file string
			line int
		)
		for {
			frame, more := frames.Next()
			if !more {
				break
			}
			fn, file, line = frame.Function, frame.File, frame.Line
		}
		if fn != "" {
			b.OnSlow("update", fn, file, line)
		}
	}
	return err
}
