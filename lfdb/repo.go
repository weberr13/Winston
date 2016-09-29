package lfdb

import (
	"fmt"
	pb "github.com/LogRhythm/Winston/pb"
	"github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
	// "github.com/davecgh/go-spew/spew"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const MonthDayYear = "01-02-2006"

//file pattern /path/to/dir/repo/01-02-2006/00.blt
type Repo struct {
	name       string
	basePath   string
	repoDir    string
	partitions *Partitions
}

type Partitions struct {
	P map[string]*Partition
	*sync.Mutex
}

type Partition struct {
	DB DB
	*sync.Mutex
	openCount int
}

var ERR_NO_PARTITION = fmt.Errorf("no partition found")

//NewRepo will create a Repo struct used to manage repo's
func NewRepo(name string, basePath string) Repo {

	return Repo{name: name,
		basePath:   basePath,
		repoDir:    fmt.Sprintf("%s/%s", basePath, name),
		partitions: &Partitions{P: make(map[string]*Partition, 0), Mutex: &sync.Mutex{}},
	}
}

func (r Repo) OpenPartition(partitionPath string) (p *Partition, err error) {
	var ok bool
	r.partitions.Lock()
	defer r.partitions.Unlock()
	p, ok = r.partitions.P[partitionPath]
	if !ok {
		p = &Partition{
			DB:    NewDB(partitionPath),
			Mutex: &sync.Mutex{},
		}
		r.partitions.P[partitionPath] = p
	}
	p.Lock()
	defer p.Unlock()
	err = p.DB.Open()
	p.openCount++

	return p, err
}

func (r Repo) ClosePartition(partitionPath string) error {
	r.partitions.Lock()
	defer r.partitions.Unlock()
	p, ok := r.partitions.P[partitionPath]
	if !ok {
		return ERR_NO_PARTITION
	}
	p.Lock()
	defer p.Unlock()
	p.openCount--
	if p.openCount == 0 {
		log.Info("partition: ", p.DB.Path, " closing, no more open transactions")
		p.DB.Close()
	} else {
		log.Info("partition: ", p.DB.Path, " transactions: ", p.openCount)
	}
	return nil
}

func (r Repo) getDatePartitionCallFunc(path string, anon func(partitionPath string) error) error {
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
func (r Repo) GetPartitionsCallFunc(start time.Time, end time.Time, anon func(partitionPath string) error) error {
	if start.After(end) {
		return fmt.Errorf("Start time is after end time")
	}
	files, err := ioutil.ReadDir(r.repoDir)
	if err != nil {
		return err
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
			err := r.getDatePartitionCallFunc(fullPath, anon)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//ReadPartition will read a single partition calling a function with a transaction
func (r Repo) ReadPartition(partitionPath string, f func(tx *bolt.Tx) error) error {
	fullPath := fmt.Sprintf("%s/%s", r.repoDir, partitionPath)
	p, err := r.OpenPartition(fullPath)
	if err != nil {
		return err
	}
	defer r.ClosePartition(fullPath)
	return p.DB.ReadView(f)
}

func (r Repo) ReadPartitionByTime(partitionPath string, readBatchSize int, startTimeMS int64, endTimeMS int64, f func(rows []*pb.Row) error) error {
	fullPath := fmt.Sprintf("%s/%s", r.repoDir, partitionPath)
	rows := make([]*pb.Row, 0, readBatchSize)
	p, err := r.OpenPartition(fullPath)
	if err != nil {
		return err
	}
	defer r.ClosePartition(fullPath)
	position := uint64(0)
	for {

		rows, position, err = p.DB.ReadN(position, readBatchSize, startTimeMS, endTimeMS)

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

//CreatePartitionIfNotExist ...
func (r Repo) CreatePartitionIfNotExist(t time.Time, basePath string, partition int) (db DB, err error) {
	date := t.Format(MonthDayYear)
	fullFolderPattern := fmt.Sprintf("%s/%s/%s", r.basePath, r.name, date)
	if err = os.MkdirAll(fullFolderPattern, 0750); err != nil {
		return db, err
	}
	filePath := fmt.Sprintf("%s/%d%s", fullFolderPattern, partition, DB_EXT)
	p, err := r.OpenPartition(filePath)
	return p.DB, err
}
