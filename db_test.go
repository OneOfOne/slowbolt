package slowbolt

import (
	"testing"
	"time"
)

func Test(t *testing.T) {
	db := &DB{
		SlowDuration: time.Second,
		OnSlow: func(op, fn, file string, line int) {
			t.Logf("%v %v %v:%v", op, fn, file, line)
		},
	}
	slowTest(db)
}

func slowTest(db *DB) {
	cfn := db.timeItCtx("Test")
	defer cfn()
	time.Sleep(time.Second * 2)
}
