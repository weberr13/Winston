package lfdb

import (
	// "fmt"
	// uuid "github.com/satori/go.uuid"
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

	// run the Fib function b.N times
	db := NewDB(name)
	db.Open()
	// boom.DeleteBucket(bucketName)
	defer db.Close()
	var rows []Row
	for i := 0; i < batch; i++ {

		row := Row{Time: time.Now(), Data: []byte("Data blah blah oi f wooooooooowfiejfojwifjwofjwiofjwoi Data blah blah oi f wooooooooowfiejfojwifjwofjwiofjwoi Data blah blah oi f wooooooooowfiejfojwifjwofjwiofjwoi")}
		rows = append(rows, row)
	}
	for n := 0; n < b.N; n++ {
		_ = db.WriteBatch(rows...)
		// var keys [][]byte
		// for _, r := range results {
		// 	keys = append(keys, r.BoomID)
		// }
		// old, more, err = boom.GetOlderThenOrEqualTo(time.Now(), bucketName)
		// boom.DeleteKeys(bucketName, keys...)
		// _, _ = boom.BucketStats(bucketName)
	}
}
