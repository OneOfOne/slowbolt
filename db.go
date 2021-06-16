package slowbolt

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
	"go.oneofone.dev/oerrs"
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
	return &DB{DB: db}, nil
}

type DB struct {
	*bbolt.DB
	SlowDuration time.Duration
	OnSlow       func(op, fn, file string, line int)
}

func (b *DB) timeItCtx(op string) func() {
	if b.SlowDuration == -1 {
		return func() {}
	}
	fn, file, line := oerrs.Caller(2).Location()
	dur := b.SlowDuration
	if dur == 0 {
		dur = time.Minute
	}
	path := filepath.Base(b.DB.Path())
	start := time.Now()
	timer := time.NewTimer(dur)
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-timer.C:
			if b.OnSlow != nil {
				b.OnSlow(op, fn, file, line)
			} else {
				log.Printf("[slowbolt:%s] %s stuck, called by %s (%s:%d)", path, op, fn, file, line)
			}
		}

		if !timer.Stop() {
			<-timer.C
		}
	}()
	return func() {
		close(done)
		took := time.Since(start)
		if took <= dur {
			return
		}
		log.Printf("[slowbolt:%s] %s took %v, called by %s (%s:%d)", path, op, took, fn, file, line)
	}
}

func (b *DB) Update(fn func(*Tx) error) error {
	cfn := b.timeItCtx("Update")
	defer cfn()
	return b.DB.Update(fn)
}

func (b *DB) View(fn func(*Tx) error) error {
	cfn := b.timeItCtx("View")
	defer cfn()
	return b.DB.View(fn)
}
