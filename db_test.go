package slowbolt

import (
	"log"
	"testing"
	"time"
)

func Test(t *testing.T) {
	log.SetFlags(log.Lshortfile)
	db := &DB{
		SlowDuration: time.Second,
		OnSlow: func(op, fn, file string, line int) {
			t.Logf("%v %v %v:%v", op, fn, file, line)
		},
	}

	slowTest(db)
}

func slowTest(db *DB) {
	go func() {
		slowTest2(db)
	}()
	afn, cfn := db.timeItCtx("Test")
	defer cfn()
	time.Sleep(time.Second * 1)
	afn()
	time.Sleep(time.Second * 4)
}

func slowTest2(db *DB) {
	afn, cfn := db.timeItCtx("Test2")
	defer cfn()
	time.Sleep(time.Second * 2)
	afn()
	time.Sleep(time.Second * 1)
}
