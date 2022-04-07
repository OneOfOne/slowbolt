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
	sync.RWMutex
	fn, file string
	extra    interface{}
	line     int
	start    time.Time
}

func getLastUpdater() (string, interface{}, time.Time) {
	lastUpdater.RLock()
	defer lastUpdater.RUnlock()
	return lastUpdater.fn, lastUpdater.extra, lastUpdater.start
}

func setLastUpdater(fr *oerrs.Frame, extra interface{}) (string, interface{}, time.Time) {
	lastUpdater.Lock()
	defer lastUpdater.Unlock()
	if fr == nil { // reset
		lastUpdater.fn, lastUpdater.file, lastUpdater.line = "", "", 0
		return "", nil, time.Time{}
	}
	if lastUpdater.fn != "" {
		return lastUpdater.fn, lastUpdater.extra, lastUpdater.start
	}
	lastUpdater.fn, lastUpdater.file, lastUpdater.line = fr.Location()
	lastUpdater.start = time.Now()
	lastUpdater.extra = extra
	return "", nil, time.Time{}
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
	reqID        int64
}

func (b *DB) timeItCtx(op string) (func(extra interface{}), func()) {
	var (
		fr = oerrs.Caller(2)

		dur            = b.SlowDuration
		path           string
		start          = time.Now()
		last           string
		held           time.Time
		timer          = time.NewTimer(dur)
		done           = make(chan struct{})
		extra          interface{}
		fn, file, line = fr.Location()
	)

	if b.DB != nil {
		path = filepath.Base(b.DB.Path())
	}

	go func() {
		select {
		case <-done:
		case <-timer.C:
			if b.OnSlow != nil {
				b.OnSlow(op, fn, file, line)
			} else {
				l, e, h := getLastUpdater()
				log.Printf("[slowbolt:%s] %s stuck, called by %s (%s:%d), lock held by %s for %s, data:%v", path, op, fn, file, line, l, time.Since(h), e)
			}
		}

		if !timer.Stop() {
			<-timer.C
		}
	}()

	afn := func(e interface{}) {
		last, extra, held = setLastUpdater(fr, e)
	}

	cfn := func() {
		close(done)
		took := time.Since(start)
		if took <= dur {
			if last == "" {
				setLastUpdater(nil, nil)
			}
			return
		}
		if last != "" {
			log.Printf("[slowbolt:%s] %s took %v, called by %s (%s:%d), lock held by %s for %s, data: %+v",
				path, op, took, fn, file, line, last, time.Since(held), extra)
			return
		}
		log.Printf("[slowbolt:%s] SLOWEST %s took %v, called by %s (%s:%d), data: %+v", path, op, took, fn, file, line, extra)
		setLastUpdater(nil, nil)
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
		afn(nil)
		return fn(t)
	})
}

func (b *DB) UpdateWithData(fn func(*Tx) error, data interface{}) error {
	if b.SlowDuration == -1 {
		return b.DB.Update(fn)
	}
	afn, cfn := b.timeItCtx("Update")
	defer cfn()
	return b.DB.Update(func(t *bbolt.Tx) error {
		afn(data)
		return fn(t)
	})
}

// func (b *DB) View(fn func(*Tx) error) error {
// 	if b.SlowDuration == -1 {
// 		return b.DB.View(fn)
// 	}
// 	afn, cfn := b.timeItCtx("View")
// 	defer cfn()
// 	return b.DB.View(func(t *bbolt.Tx) error {
// 		afn()
// 		return fn(t)
// 	})
// }
