package lfdb

import (
	"encoding/binary"
	"github.com/boltdb/bolt"
	"time"
)

type DB struct {
	Path string
	db   *bolt.DB
}

type Row struct {
	Data []byte
	Time time.Time
}

func NewDB(path string) DB {
	return DB{Path: path}
}

func (d *DB) Open() (err error) {
	if d.db == nil {
		d.db, err = bolt.Open(d.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	}
	return err
}

func (d DB) WriteBatch(row ...Row) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		//create sub bucket for time
		tb, err := b.CreateBucketIfNotExists([]byte("time"))
		if err != nil {
			return err
		}
		for _, r := range row {
			id, err := b.NextSequence()
			if err != nil {
				return err
			}
			key := U64ToBytes(id)
			if err = b.Put(key, r.Data); err != nil {
				return err
			}
			if err = tb.Put(key, U64ToBytes(uint64(r.Time.UnixNano()))); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func U64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func (d DB) Close() error {
	return d.db.Close()
}
