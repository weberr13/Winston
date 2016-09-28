package lfdb

import (
	// "errors"
	"fmt"
	log "github.com/cihub/seelog"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const REPO_TEST_DIR = "winston_repo_test"
const TEMP_PREFIX = "winston_repo_test"

func TestRepo(t *testing.T) {
	DeleteOldTmpFiles()
	defer log.Flush()
	Convey("GetPartitionsCallFunc - find correct buckets", t, func() {
		now := time.Now().UTC()
		r := CreateRepoWithFakeFiles("repo", 5, now)
		cnt := 0
		Today := TimeRoundToDay(now)
		r.GetPartitionsCallFunc(Today, Today, func(path string) error {
			log.Info(path)
			cnt++
			return nil
		})
		So(cnt, ShouldEqual, 5)
	})

	Convey("GetPartitionsCallFunc - ignores folders", t, func() {
		now := time.Now().UTC()
		r := CreateRepoWithFakeFiles("repo", 5, now)
		nowFormat := now.Format(MonthDayYear)
		folderPath := fmt.Sprintf("%s/%s/6%s", r.repoDir, nowFormat, DB_EXT)
		err := os.Mkdir(folderPath, 0777)
		So(err, ShouldBeNil)
		cnt := 0
		Today := TimeRoundToDay(now)
		r.GetPartitionsCallFunc(Today, Today, func(path string) error {
			log.Info(path)
			cnt++
			return nil
		})
		So(cnt, ShouldEqual, 5)
	})

	Convey("CreatePartitionIfNotExist - create a new bucket", t, func() {
		r := NewRepo("repo", GetTestDir())
		dir, err := ioutil.TempDir("", TEMP_PREFIX)
		So(err, ShouldBeNil)
		db, err := r.CreatePartitionIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		err = db.Open()
		defer db.Close()
		So(err, ShouldBeNil)

	})

	Convey("CreatePartitionIfNotExist - create same bucket twice", t, func() {
		r := NewRepo("repo", GetTestDir())
		dir, err := ioutil.TempDir("", TEMP_PREFIX)
		So(err, ShouldBeNil)
		db, err := r.CreatePartitionIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		db, err = r.CreatePartitionIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		err = db.Open()
		defer db.Close()
		So(err, ShouldBeNil)

	})

}

func TestRepoPartitions(t *testing.T) {
	DeleteOldTmpFiles()
	defer log.Flush()

	Convey("OpenPartition - open a partition and close it", t, func() {
		now := time.Now()
		today := TimeRoundToDay(now)
		r := CreateRepoWithFakeFiles("partition_test", 1, now)
		r.GetPartitionsCallFunc(today, today, func(path string) error {
			partitionPath := r.repoDir + "/" + path
			_, err := r.OpenPartition(partitionPath)
			So(err, ShouldBeNil)
			err = r.ClosePartition(partitionPath)
			So(err, ShouldBeNil)
			return nil
		})

	})

	Convey("OpenPartition - open a but close wrong partition", t, func() {
		now := time.Now()
		today := TimeRoundToDay(now)
		r := CreateRepoWithFakeFiles("partition_test", 1, now)
		r.GetPartitionsCallFunc(today, today, func(path string) error {
			partitionPath := r.repoDir + "/" + path
			_, err := r.OpenPartition(partitionPath)
			So(err, ShouldBeNil)
			err = r.ClosePartition("invalid_path")
			So(err, ShouldNotBeNil)
			return nil
		})
	})

	Convey("OpenPartition - open many times from many threads and validate close is clean", t, func() {
		now := time.Now()
		today := TimeRoundToDay(now)
		r := CreateRepoWithFakeFiles("partition_test", 1, now)
		wg := &sync.WaitGroup{}
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.GetPartitionsCallFunc(today, today, func(path string) error {
					partitionPath := r.repoDir + "/" + path
					_, err := r.OpenPartition(partitionPath)
					if err != nil {
						t.FailNow()
					}
					err = r.ClosePartition(partitionPath)
					if err != nil {
						t.FailNow()
					}
					return nil
				})

			}()
		}
		wg.Wait()
		r.partitions.Lock()
		defer r.partitions.Unlock()
		for _, v := range r.partitions.P {
			v.Lock()
			So(v.openCount, ShouldEqual, 0)
			v.Unlock()
		}
	})
}

func DeleteOldTmpFiles() {
	tempDirPattern := fmt.Sprintf("%s/%s*", os.TempDir(), TEMP_PREFIX)
	log.Info("removing temp dirs that match this pattern: ", tempDirPattern)
	files, err := filepath.Glob(tempDirPattern)
	if err != nil {
		log.Error("failed to find temp dirs: ", err)
		return
	}

	for _, f := range files {
		err := os.RemoveAll(f)
		if err != nil {
			log.Error("Failed to remove temp dir: ", f, " error ", err)
		}
	}

}

func CreateRepoWithFakeFiles(repo string, buckets int, now time.Time) Repo {
	r := NewRepo(repo, GetTestDir())
	nowFormat := now.Format(MonthDayYear)
	tempFull := fmt.Sprintf("%s/%s", r.repoDir, nowFormat)
	os.MkdirAll(tempFull, 0777)
	for i := 0; i < buckets; i++ {
		os.Create(fmt.Sprintf("%s/%d%s", tempFull, i, DB_EXT))
	}
	return r

}

func TimeRoundToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

func GetTestDir() string {
	dir, err := ioutil.TempDir("", REPO_TEST_DIR)
	if err != nil {
		panic(fmt.Sprintf("Failed to get winston conf for test: %v", err))
	}
	return dir
}
