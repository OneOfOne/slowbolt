package slowbolt

import (
	"log"
	"testing"
	"time"
)

func Test(t *testing.T) {
	db, err := Open(t.TempDir()+"/test.db", 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.SlowDuration = time.Second
	db.OnSlow = func(op, fn, file string, line int) {
		t.Log(op, fn, file, line)
	}

	log.SetFlags(log.Lshortfile)
	slowTest(db)
}

func slowTest(db *DB) {
	go func() {
		slowTest2(db)
	}()
	db.Update(func(tx *Tx) error {
		time.Sleep(time.Second * 2)
		return nil
	})
	db.Update(func(tx *Tx) error {
		time.Sleep(time.Second * 3)
		return nil
	})
}

func slowTest2(db *DB) {
	db.Update(func(tx *Tx) error {
		time.Sleep(time.Second * 1)
		return nil
	})
	db.Update(func(tx *Tx) error {
		time.Sleep(time.Second * 2)
		return nil
	})
}
