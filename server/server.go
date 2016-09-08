package server

import (
	LFDB "github.com/LogRhythm/Winston/lfdb"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	// "github.com/davecgh/go-spew/spew"
	"github.com/tidwall/gjson"
	"stablelib.com/v1/crypto/siphash"
	// "golang.org/x/net/context"
	"fmt"
	"google.golang.org/grpc"
	// "google.golang.org/grpc/grpclog"
	"io"
	"net"
	"time"
)

type Winston struct {
	server *grpc.Server
	listen net.Listener
}

func NewWinston() Winston {
	grpc.EnableTracing = true
	// flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 5001))
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	var opts []grpc.ServerOption
	// if *tls {
	// 	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	// 	if err != nil {
	// 		grpclog.Fatalf("Failed to generate credentials %v", err)
	// 	}
	// 	opts = []grpc.ServerOption{grpc.Creds(creds)}
	// }
	w := Winston{server: grpc.NewServer(opts...), listen: lis}
	pb.RegisterV1Server(w.server, w)
	return w
}

func (w Winston) Start() {
	go w.server.Serve(w.listen)
}

type RowsBucketedByBucketAndDate []map[time.Time][]LFDB.Row

func (w Winston) Push(stream pb.V1_PushServer) error {
	bucketRows := make(RowsBucketedByBucketAndDate, 65536)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			for bucket, timeMap := range bucketRows {
				for rowTime, rows := range timeMap {
					log.Info("Calling Write")
					err := WriteToDB(bucket, "something", rowTime, rows)
					if nil != err {
						return err
					}
				}
			}
			stream.SendAndClose(&pb.PushResponse{})
			// log.Info("Done")
			return nil //done with stream
		}
		// spew.Dump(in)
		if err != nil {
			log.Error("failed transaction: ", err)
			return err
		}
		if len(in.Repo) == 0 {
			return fmt.Errorf("Empty repo name")
		}
		bucket := uint64(0)
		if in.Buckets != 0 {
			value := gjson.Get(string(in.Data), "id")
			hash := siphash.Hash(0, 65536, []byte(value.String()))
			bucket = hash % uint64(in.Buckets)
			// fmt.Println("ID: ", value.String())
		}
		// fmt.Println("hash: ", bucket)
		var t time.Time
		if in.Time == 0 {
			t = time.Now()
		} else {
			t = time.Unix(int64(in.Time), 0) //time in seconds
		}

		if bucketRows[bucket] == nil {
			bucketRows[bucket] = make(map[time.Time][]LFDB.Row, 0)
		}
		mapTime := t.Truncate(24 * time.Hour)
		if bucketRows[bucket][mapTime] == nil {
			bucketRows[bucket][mapTime] = make([]LFDB.Row, 0)
		}
		bucketRows[bucket][mapTime] = append(bucketRows[bucket][mapTime], LFDB.Row{Time: t, Data: in.Data})
	}

}

func WriteToDB(bucket int, repoName string, t time.Time, rows []LFDB.Row) error {
	repo := LFDB.NewRepo(repoName)
	db, err := repo.CreateBucketIfNotExist(t, "/Users/vrecan/winston", int(bucket))
	log.Info("db name: ", db.Path)
	if nil != err {
		return err
	}
	err = db.Open()
	if err != nil {
		return err
	}

	err = db.WriteBatch(rows...)
	if err != nil {
		return err
	}
	err = db.Close()
	return err
}

func (w Winston) Close() error {
	if w.server != nil {
		w.server.GracefulStop()
	}
	return nil
}
