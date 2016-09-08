package lfdb

import (
	// "errors"
	"fmt"
	log "github.com/cihub/seelog"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestRepo(t *testing.T) {
	defer log.Flush()
	Convey("GetBucketPathsForDate - find correct buckets", t, func() {
		r := Repo{Name: "test_name"}
		now := time.Now()
		nowFormat := now.Format(MonthDayYear)
		temp, err := ioutil.TempDir("", "repo_test")
		So(err, ShouldBeNil)
		tempFull := fmt.Sprintf("%s/%s/%s", temp, r.Name, nowFormat)
		os.MkdirAll(tempFull, 0777)
		os.Create(fmt.Sprintf("%s/00.blt", tempFull))
		os.Create(fmt.Sprintf("%s/01.blt", tempFull))
		os.Create(fmt.Sprintf("%s/02.blt", tempFull))
		os.Create(fmt.Sprintf("%s/03.blt", tempFull))
		os.Create(fmt.Sprintf("%s/04.blt", tempFull))
		buckets, err := r.GetBucketPathsForDate(now, temp)
		So(err, ShouldBeNil)
		So(len(buckets), ShouldEqual, 5)
	})

	Convey("CreateBucketIfNotExist - create a new bucket", t, func() {
		r := Repo{Name: "create"}
		dir, err := ioutil.TempDir("", "repo_test")
		So(err, ShouldBeNil)
		db, err := r.CreateBucketIfNotExist(time.Now(), dir, 255)
		So(err, ShouldBeNil)
		err = db.Open()
		defer db.Close()
		So(err, ShouldBeNil)

	})

	Convey("CreateBucketIfNotExist - create same bucket twice", t, func() {
		r := Repo{Name: "create"}
		dir, err := ioutil.TempDir("", "repo_test")
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
