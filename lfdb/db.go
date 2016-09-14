package lfdb

import (
	"encoding/binary"
	"fmt"
	SNAP "github.com/LogRhythm/Winston/snappy"
	"github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
	"time"
)

//DB is the data structure for storing our database (boltdb)
type DB struct {
	Path string
	db   *bolt.DB
}

//Row is a single row in the db.
type Row struct {
	Data []byte
	Time time.Time
}

//NewDB will create a new database using the path for the file that it will create.
func NewDB(path string) DB {
	return DB{Path: path}
}

//Open the boltdb database
func (d *DB) Open() (err error) {
	if d.db == nil {
		//long long time but fail eventually
		d.db, err = bolt.Open(d.Path, 0600, &bolt.Options{Timeout: 30 * time.Minute})
	}
	return err
}

//WriteBatch will write all supplied rows into a batch before exiting.
func (d DB) WriteBatch(rows ...Row) error {
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

		for _, r := range rows {
			id, err := b.NextSequence()
			if err != nil {
				return err
			}
			key := U64ToBytes(id)
			//if we decide to change this we will have to support multiple format versions
			if err = b.Put(key, SNAP.CompressBytes(r.Data)); err != nil {
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

//Write a key with a value to the db, this will overwrite anything with the same key
func (d DB) WriteKey(key string, data []byte, bucket string) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		//if we decide to change this we will have to support multiple format versions
		if err = b.Put([]byte(key), SNAP.CompressBytes(data)); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (d DB) ReadView(f func(tx *bolt.Tx) error) error {
	return d.db.View(f)
}

func (d DB) ReadKey(key string, bucket string) (value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			log.Info("Bucket ", bucket, " does not exist")
			return fmt.Errorf("Bucket does not exist: ", bucket)
		}
		//if we decide to change this we will have to support multiple format versions
		v, err := SNAP.DecompressBytes(b.Get([]byte(key)))
		if nil != err {
			log.Error("Failed to decompress value from key: ", key)
			return err
		}
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	log.Info("value: ", value)
	return value, err
}

//U64ToBytes will convert any unt64 into a padding BigEndian set of bytes (8bytes)
func U64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

//Close ...
func (d DB) Close() error {
	if d.db != nil {
		err := d.db.Close()
		d.db = nil
		return err
	}
	return nil
}
