package lfdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const MonthDayYear = "01-02-2006"

//file pattern /path/to/dir/repo/01-02-2006/00.blt
type Repo struct {
	Name string
}

func NewRepo(name string) Repo {
	return Repo{Name: name}
}

func (r Repo) GetBucketPathsForDate(t time.Time, basePath string) ([]DB, error) {
	date := t.Format(MonthDayYear)
	buckets := make([]DB, 0)
	fullFolderPattern := fmt.Sprintf("%s/%s/%s/", basePath, r.Name, date)
	files, err := ioutil.ReadDir(fullFolderPattern)
	if err != nil {
		return buckets, err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		ext := filepath.Ext(f.Name())
		if ext == ".blt" {
			buckets = append(buckets, DB{Path: fmt.Sprintf("%s/%s/%s/%s", basePath, r.Name, date, f.Name())})
		}
	}
	return buckets, err
}

func (r Repo) CreateBucketIfNotExist(t time.Time, basePath string, bucket int) (db DB, err error) {
	date := t.Format(MonthDayYear)
	fullFolderPattern := fmt.Sprintf("%s/%s/%s/", basePath, r.Name, date)
	if err = os.MkdirAll(fullFolderPattern, 0750); err != nil {
		return db, err
	}
	filePath := fmt.Sprintf("%s/%d.blt", fullFolderPattern, bucket)
	db = NewDB(filePath)
	return db, err
}
