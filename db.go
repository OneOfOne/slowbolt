package slowbolt

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"go.oneofone.dev/oerrs"
)

var lastUpdater struct {
	sync.Mutex
	fn, file string
	line     int
	start    time.Time
}

func setLastUpdater(fr *oerrs.Frame) (string, time.Time) {
	lastUpdater.Lock()
	defer lastUpdater.Unlock()
	if fr == nil { // reset
		lastUpdater.fn, lastUpdater.file, lastUpdater.line = "", "", 0
		return "", time.Time{}
	}
	if lastUpdater.fn != "" {
		return lastUpdater.fn, lastUpdater.start
	}
	lastUpdater.fn, lastUpdater.file, lastUpdater.line = fr.Location()
	lastUpdater.start = time.Now()
	return "", time.Time{}
}

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
	SlowDuration time.Duration
	OnSlow       func(op, fn, file string, line int)
}

func (b *DB) timeItCtx(op string) (func(), func()) {
	var (
		fr = oerrs.Caller(2)

		dur   = b.SlowDuration
		path  = filepath.Base(b.DB.Path())
		start = time.Now()
		last  string
		held  time.Time
		timer = time.NewTimer(dur)
		done  = make(chan struct{})

		fn, file, line = fr.Location()
	)

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
	afn := func() {
		last, held = setLastUpdater(fr)
	}
	cfn := func() {
		close(done)
		took := time.Since(start)
		if took <= dur {
			if last == "" {
				setLastUpdater(nil)
			}
			return
		}
		if last != "" {
			log.Printf("[slowbolt:%s] %s took %v, called by %s (%s:%d), lock held by %s for %s",
				path, op, took, fn, file, line, last, time.Since(held))
			return
		}
		log.Printf("[slowbolt:%s] SLOWEST %s took %v, called by %s (%s:%d)", path, op, took, fn, file, line)
		setLastUpdater(nil)
	}
	return afn, cfn
}

func (b *DB) Update(fn func(*Tx) error) error {
	if b.SlowDuration == -1 {
		return b.DB.Update(fn)
	}
	afn, cfn := b.timeItCtx("Update")
	defer cfn()
	return b.DB.Update(func(t *bbolt.Tx) error {
		afn()
		return fn(t)
	})
}

func (b *DB) View(fn func(*Tx) error) error {
	if b.SlowDuration == -1 {
		return b.DB.View(fn)
	}
	afn, cfn := b.timeItCtx("View")
	defer cfn()
	return b.DB.View(func(t *bbolt.Tx) error {
		afn()
		return fn(t)
	})
}
