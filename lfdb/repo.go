package lfdb

import (
	"fmt"
	"github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
	"io/ioutil"
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

func (r Repo) GetBucketsCallFunc(start time.Time, end time.Time, anon func(bucket string) error) error {
	files, err := ioutil.ReadDir(r.repoDir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		fullPath := fmt.Sprintf("%s/%s", r.repoDir, f.Name())
		log.Info("PATH: ", fullPath)
		t, err := time.Parse(MonthDayYear, f.Name())
		if err != nil {
			//ignore folders that aren't ours
			continue
		}
		if (t.After(start) || t.Equal(start)) && (t.Before(end) || t.Equal(end)) {
			log.Info("PATH GOOD: ", f.Name())
			err := r.getDateBucketCallFunc(fullPath, anon)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r Repo) ReadBucket(bucketPath string, f func(tx *bolt.Tx) error) error {
	db := NewDB(bucketPath)
	err := db.Open()
	defer db.Close()
	if err != nil {
		return err
	}
	return db.ReadView(f)
}

func (r Repo) CreateBucketIfNotExist(t time.Time, basePath string, bucket int) (db DB, err error) {
	date := t.Format(MonthDayYear)
	fullFolderPattern := fmt.Sprintf("%s/%s/%s/", r.basePath, r.name, date)
	if err = os.MkdirAll(fullFolderPattern, 0750); err != nil {
		return db, err
	}
	filePath := fmt.Sprintf("%s/%d.%s", fullFolderPattern, bucket, DB_EXT)
	db = NewDB(filePath)
	return db, err
}
