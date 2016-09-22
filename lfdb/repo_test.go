package lfdb

import (
	// "errors"
	"fmt"
	log "github.com/cihub/seelog"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const REPO_TEST_DIR = "winston_repo_test"
const TEMP_PREFIX = "winston_repo_test"

func TestRepo(t *testing.T) {
	DeleteOldTmpFiles()
	defer log.Flush()
	Convey("GetBucketsCallFunc - find correct buckets", t, func() {
		now := time.Now().UTC()
		r := CreateRepoWithFakeFiles("repo", 5, now)
		cnt := 0
		Today := TimeRoundToDay(now)
		r.GetBucketsCallFunc(Today, Today, func(path string) error {
			log.Info(path)
			cnt++
			return nil
		})
		So(cnt, ShouldEqual, 5)
	})

	Convey("GetBucketsCallFunc - ignores folders", t, func() {
		now := time.Now().UTC()
		r := CreateRepoWithFakeFiles("repo", 5, now)
		nowFormat := now.Format(MonthDayYear)
		folderPath := fmt.Sprintf("%s/%s/6%s", r.repoDir, nowFormat, DB_EXT)
		err := os.Mkdir(folderPath, 0777)
		So(err, ShouldBeNil)
		cnt := 0
		Today := TimeRoundToDay(now)
		r.GetBucketsCallFunc(Today, Today, func(path string) error {
			log.Info(path)
			cnt++
			return nil
		})
		So(cnt, ShouldEqual, 5)
	})

	Convey("CreateBucketIfNotExist - create a new bucket", t, func() {
		r := NewRepo("repo", GetTestDir())
		dir, err := ioutil.TempDir("", TEMP_PREFIX)
		So(err, ShouldBeNil)
		db, err := r.CreateBucketIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		err = db.Open()
		defer db.Close()
		So(err, ShouldBeNil)

	})

	Convey("CreateBucketIfNotExist - create same bucket twice", t, func() {
		r := NewRepo("repo", GetTestDir())
		dir, err := ioutil.TempDir("", TEMP_PREFIX)
		So(err, ShouldBeNil)
		db, err := r.CreateBucketIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		db, err = r.CreateBucketIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		err = db.Open()
		defer db.Close()
		So(err, ShouldBeNil)

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
	r := NewRepo("repo", GetTestDir())
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
