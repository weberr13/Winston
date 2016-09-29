package lfdb

import (
	"encoding/binary"
	"fmt"
	pb "github.com/LogRhythm/Winston/pb"
	SNAP "github.com/LogRhythm/Winston/snappy"
	"github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
	"io"
	// TIME "github.com/LogRhythm/Winston/time"
	"time"
)

const (
	DB_EXT = ".bolt"
)

var BUCKET_DATA = []byte("data")
var BUCKET_TIME = []byte("time")

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
		log.Info("Opening db: ", d.Path)
		d.db, err = bolt.Open(d.Path, 0600, &bolt.Options{Timeout: 30 * time.Minute})
	}
	return err
}

//OpenReadOnly sets the option to read the boltdb with readonly permissions
func (d *DB) OpenReadOnly() (err error) {
	if d.db == nil {
		//long long time but fail eventually
		log.Info("Opening read only: ", d.Path)
		d.db, err = bolt.Open(d.Path, 0600, &bolt.Options{Timeout: 30 * time.Minute, ReadOnly: true})
	}
	return err
}

//WriteBatch will write all supplied rows into a batch before exiting.
func (d DB) WriteBatch(rows ...Row) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(BUCKET_DATA)
		if err != nil {
			return err
		}
		//create sub bucket for time
		tb, err := b.CreateBucketIfNotExists(BUCKET_TIME)
		if err != nil {
			return err
		}

		for _, r := range rows {
			id, err := b.NextSequence()
			if err != nil {
				return err
			}
			key := U64ToPaddedBytes(id)
			//if we decide to change this we will have to support multiple format versions
			if err = b.Put(key, SNAP.CompressBytes(r.Data)); err != nil {
				return err
			}
			//put into time bucket
			if err = tb.Put(key, U64ToPaddedBytes(uint64(r.Time.UnixNano()))); err != nil {
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

//ReadView allows you to write your own read only bolt function.
func (d DB) ReadView(f func(tx *bolt.Tx) error) error {
	return d.db.View(f)
}

//ReadN will read n number of records and return
func (d DB) ReadN(startPosition uint64, n int, startTimeMS int64, endTimeMS int64) (rows []*pb.Row, position uint64, err error) {
	if startPosition < 0 {
		return rows, position, fmt.Errorf("negative start position")
	}
	rows = make([]*pb.Row, 0, n)
	finished := false
	err = d.ReadView(func(tx *bolt.Tx) error {
		b := tx.Bucket(BUCKET_DATA)

		if b == nil {
			return fmt.Errorf("data bucket does not exist")
		}
		tb := b.Bucket(BUCKET_TIME)
		if nil == tb {
			return fmt.Errorf("time bucket doesn't exist")
		}

		tc := tb.Cursor()
		bc := b.Cursor()
		for k, v := tc.Seek(U64ToPaddedBytes(startPosition)); k != nil; k, v = tc.Next() {
			if len(k) != 8 {
				log.Error("Invalid key in db")
				continue
			}
			position = binary.BigEndian.Uint64(k)
			rt := binary.BigEndian.Uint64(v)
			if rt >= uint64(startTimeMS) && rt <= uint64(endTimeMS) {

				n--
				_, bv := bc.Seek(k)
				if bv == nil {
					continue
				}
				value, err := SNAP.DecompressBytes(bv)
				if err != nil {
					return err
				}

				rows = append(rows, &pb.Row{TimeMs: rt, Data: value})
				if n == 0 {
					return nil
				}
			}
		}
		finished = true
		return nil
	})
	if finished {
		err = io.EOF
	}
	return rows, position, err

}

func (d DB) ReadKey(key string, bucket string) (value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			log.Info("Bucket ", bucket, " does not exist")
			return fmt.Errorf("Bucket does not exist: %v", bucket)
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
func U64ToPaddedBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

//Close ...
func (d *DB) Close() error {
	if d == nil {
		return nil
	}
	// log.Debug("CLOSING: ", d)
	if d.db != nil {
		err := d.db.Close()
		d.db = nil
		// log.Debug("CLOSED: ", d)
		return err
	}
	return nil
}
