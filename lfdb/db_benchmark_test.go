package lfdb

import (
	"fmt"
	"io"
	"testing"
	"time"
)

// var old []Old
var more bool
var err error

func BenchmarkBasicWorkLoad10(b *testing.B) {
	RunWorkload("test3", 10, b)
}

func BenchmarkBasicWorkLoad100(b *testing.B) {
	RunWorkload("test4", 100, b)
}

func BenchmarkBasicWorkLoad1000(b *testing.B) {
	RunWorkload("test5", 1000, b)
}
func BenchmarkBasicWorkLoad10000(b *testing.B) {
	RunWorkload("test6", 10000, b)
}

func RunWorkload(name string, batch int, b *testing.B) {
	db := NewDB(name)
	if db.Open() != nil {
		b.FailNow()
	}
	defer db.Close()
	var rows []Row
	startTime := time.Now()
	for i := 0; i < batch; i++ {

		row := Row{Time: time.Now(), Data: []byte("Data blah blah oi f wooooooooowfiejfojwifjwofjwiofjwoi Data blah blah oi f wooooooooowfiejfojwifjwofjwiofjwoi Data blah blah oi f wooooooooowfiejfojwifjwofjwiofjwoi")}
		rows = append(rows, row)
	}
	position := uint64(0)
	for n := 0; n < b.N; n++ {
		err = db.WriteBatch(rows...)
		if err != nil {
			panic("Error durring write")
		}

		for {
			records, newPosition, err := db.ReadN(position, batch, startTime.UnixNano(), time.Now().UnixNano())
			if err == io.EOF {
				break
			}
			position = newPosition
			if len(records) == 0 {
				fmt.Printf("recordCnt: %v, position: %v, err: %v", len(records), position, err)
				panic("Invalid record count of 0")
			}
			if position < uint64(batch) {
				panic("Invalid position")
			}
			if err != nil {
				panic("got unexpected error")
			}
			fmt.Printf("recordCnt: %v, position: %v, err: %v", len(records), position, err)
			// fmt.Printf("recordCnt: %v, position: %v, err: %v", len(records), position, err)
		}
		// log.Info(fmt.Sprintf("R: %v P: %v err: %v", len(records), position, err))
	}

}
