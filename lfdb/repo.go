package lfdb

import (
	"fmt"
	"github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
	pb "github.com/LogRhythm/Winston/pb"
	// "github.com/davecgh/go-spew/spew"
	"io/ioutil"
	"io"
	"os"
	"path/filepath"
	"time"
)

const MonthDayYear = "01-02-2006"

//file pattern /path/to/dir/repo/01-02-2006/00.blt
type Repo struct {
	name     string
	basePath string
	repoDir  string
}

//NewRepo will create a Repo struct used to manage repo's
func NewRepo(name string, basePath string) Repo {
	return Repo{name: name, basePath: basePath, repoDir: fmt.Sprintf("%s/%s", basePath, name)}
}

func (r Repo) getDateBucketCallFunc(path string, anon func(bucket string) error) error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		ext := filepath.Ext(f.Name())
		if ext == DB_EXT {
			fullPath := fmt.Sprintf("%s/%s", path, f.Name())
			relativePath, err := filepath.Rel(r.repoDir, fullPath)
			if err != nil {
				return err
			}
			err = anon(relativePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//GetBucketsCallFunc will call an function for every bucket it finds.
func (r Repo) GetBucketsCallFunc(start time.Time, end time.Time, anon func(bucket string) error) error {
	files, err := ioutil.ReadDir(r.repoDir)
	if err != nil {
		return err
	}
	if start.After(end) {
		return fmt.Errorf("Start time is after end time")
	}
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		fullPath := fmt.Sprintf("%s/%s", r.repoDir, f.Name())
		log.Debug("PATH: ", fullPath)
		log.Debug("START: ", start)
		log.Debug("END: ", end)
		t, err := time.Parse(MonthDayYear, f.Name())
		if err != nil {
			//ignore folders that aren't ours
			continue
		}
		if (t.After(start) || t.Equal(start)) && (t.Before(end) || t.Equal(end)) {
			log.Debug("PATH GOOD: ", f.Name())
			err := r.getDateBucketCallFunc(fullPath, anon)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//ReadBucket will read a single bucket calling a function with a transaction
func (r Repo) ReadBucket(bucketPath string, f func(tx *bolt.Tx) error) error {
	fullPath := fmt.Sprintf("%s/%s", r.repoDir, bucketPath)
	db := NewDB(fullPath)
	err := db.Open()
	defer db.Close()
	if err != nil {
		return err
	}
	return db.ReadView(f)
}

func (r Repo) ReadPartition(bucketPath string,readBatchSize int, startTimeMS int64, endTimeMS int64, f func(rows []*pb.Row)error) error {
	fullPath := fmt.Sprintf("%s/%s", r.repoDir, bucketPath)
	db := NewDB(fullPath)
	rows := make([]*pb.Row, 0, readBatchSize)
	err := db.OpenReadOnly()
	if err != nil {
		return err
	}
	defer db.Close()
	position := uint64(0)
	for {
		
		rows, position, err = db.ReadN(position,readBatchSize, startTimeMS, endTimeMS)

		if err == io.EOF {
			log.Info("Finished read bucket by time request")
			break
		}
		if err != nil {
			return err
		}


		err = f(rows)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
	}
	return f(rows)
} 
//CreateBucketIfNotExist ...
func (r Repo) CreateBucketIfNotExist(t time.Time, basePath string, bucket int) (db DB, err error) {
	date := t.Format(MonthDayYear)
	fullFolderPattern := fmt.Sprintf("%s/%s/%s/", r.basePath, r.name, date)
	if err = os.MkdirAll(fullFolderPattern, 0750); err != nil {
		return db, err
	}
	filePath := fmt.Sprintf("%s/%d%s", fullFolderPattern, bucket, DB_EXT)
	db = NewDB(filePath)
	return db, err
}
