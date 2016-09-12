package server

import (
	LFDB "github.com/LogRhythm/Winston/lfdb"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	ENV "github.com/joho/godotenv"
	"encoding/binary"
	// "github.com/davecgh/go-spew/spew"
	"fmt"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"stablelib.com/v1/crypto/siphash"
	// "google.golang.org/grpc/grpclog"
	"github.com/boltdb/bolt"
	"io"
	"net"
	"time"
)

//Winston
type Winston struct {
	server          *grpc.Server
	listen          net.Listener
	dataDir         string
	RepoSettingPath string
}

const settingsBucket = "v1_settings"

//NewWinston creates the winston server
func NewWinston() Winston {
	grpc.EnableTracing = true
	env, err := ENV.Read()
	if nil != err {
		panic("Failed to load environment variables, check your .env file in your current working directory.")
	}
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
	w := Winston{server: grpc.NewServer(opts...), listen: lis, dataDir: env["DATA_DIR"]}

	if len(w.dataDir) == 0 {
		panic("No DATA_DIR set in your .env file")
	}
	w.RepoSettingPath = fmt.Sprintf("%s/settings.blt", w.dataDir)
	pb.RegisterV1Server(w.server, w)
	return w
}

//Start ...
func (w Winston) Start() {
	go w.server.Serve(w.listen)
}

type RowsBucketedByBucketAndDate []map[time.Time][]LFDB.Row

//Push data into winston db.
func (w Winston) Push(stream pb.V1_PushServer) error {
	var settings *pb.RepoSettings
	bucketRows := make(RowsBucketedByBucketAndDate, 65536)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			for bucket, timeMap := range bucketRows {
				for rowTime, rows := range timeMap {
					log.Info("write")
					err = w.WriteToDB(*settings, bucket, rowTime, rows)
					if nil != err {
						return err
					}
				}
			}
			return stream.SendAndClose(&pb.EMPTY{})

		}
		// spew.Dump(in)
		if err != nil {
			log.Error("transaction: ", err)
			return err
		}
		if len(in.Repo) == 0 {
			return fmt.Errorf("empty repo name")
		}

		if settings == nil {
			settings, err = w.getSettingsForRepo(in.Repo)
			if err != nil {
				log.Error("get settings: ", err)
				return err
			}
		}

		for _, r := range in.Rows {
			bucket := uint64(0)
			if settings.Buckets != 0 {
				value := gjson.Get(string(r.Data), settings.HashField)
				hash := siphash.Hash(0, 65536, []byte(value.String()))
				bucket = hash % uint64(settings.Buckets)
				// fmt.Println("buckets: ", settings.Buckets)
			}
			// fmt.Println("hash: ", bucket)
			var t time.Time
			if r.Time == 0 {
				t = time.Now()
			} else {
				t = time.Unix(0, int64(r.Time)) //time in nanoseconds
			}

			if bucketRows[bucket] == nil {
				bucketRows[bucket] = make(map[time.Time][]LFDB.Row, 0)
			}
			mapTime := t.Truncate(24 * time.Hour)
			if bucketRows[bucket][mapTime] == nil {
				bucketRows[bucket][mapTime] = make([]LFDB.Row, 0)
			}
			bucketRows[bucket][mapTime] = append(bucketRows[bucket][mapTime], LFDB.Row{Time: t, Data: r.Data})
		}
	}
}

// func (s *routeGuideServer) ListFeatures(rect *pb.Rectangle, stream pb.RouteGuide_ListFeaturesServer) error {
// 	for _, feature := range s.savedFeatures {
// 		if inRange(feature.Location, rect) {
// 			if err := stream.Send(feature); err != nil {
// 				return err
// 			}
// 		}
// 	}
// 	return nil
// }

func (w Winston) PullBucketByTime(pull *pb.PullBucket, stream pb.V1_PullBucketByTimeServer) error {
	if pull == nil {
		return fmt.Errorf("Invalid request")
	}
	settings, err := w.getSettingsForRepo(pull.Repo)
	if err != nil {
		return fmt.Errorf("Failed to get settings: ", err)
	}
	repo := LFDB.NewRepo(settings.Repo)
	err  = repo.ReadBucket(pull.BucketPath, func(tx *bolt.Tx)error {
		b := tx.Bucket([]byte("data"))
		count :=0
		if b == nil {
			return fmt.Errorf("Bucket does not exist")
		}
		tb := b.Bucket([]byte("time"))
		if nil == tb {
			return fmt.Errorf("Bucket doesn't exist")
		}
				tb.ForEach(func(key, v []byte) error {

			rt := binary.BigEndian.Uint64(v)
			if rt >= pull.StartTime && rt <= pull.EndTime {
				count++
				stream.Send(&pb.PullResponse{Repo: settings.Repo, Rows: []*pb.Row{
					0: &pb.Row{Time: rt, Data: b.Get(key)},
				}})
			}
			log.Info("Read ", count, " records from db")
			return nil
		})
		return nil

	})
	return err

}

func (w Winston) getSettingsForRepo(repo string) (settings *pb.RepoSettings, err error) {
	settings = &pb.RepoSettings{}
	db, err := w.openSettingsDB()
	defer db.Close()
	if err != nil {
		log.Error("Failed to open settings db: ", err)
		return settings, err
	}
	bytes, err := db.ReadKey(repo, settingsBucket)
	if err != nil {
		return settings, err
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("No settings for repo: %s", repo)
	}
	err = settings.Unmarshal(bytes)
	return settings, err
}

//Upsert will create or update a repo with the supplied config
func (w Winston) UpsertRepo(ctx context.Context, settings *pb.RepoSettings) (*pb.EMPTY, error) {
	db, err := w.openSettingsDB()
	defer db.Close()
	if err != nil {
		log.Error("Failed to open settings db: ", err)
		return nil, err
	}
	if settings == nil || len(settings.Repo) == 0 {
		return nil, fmt.Errorf("failed to set repo name")
	}
	if settings.Buckets > 1024 {
		return nil, fmt.Errorf("Bucket size set to greater then 1024")
	}
	data, err := settings.Marshal()
	if nil != err {
		log.Error("Failed to marshal conf: ", err)
		return nil, err
	}
	log.Info("Updating repo: ", settings.Repo, " with values: ", string(data))
	err = db.WriteKey(settings.Repo, data, settingsBucket)
	return &pb.EMPTY{}, err
}

func (w Winston) openSettingsDB() (db LFDB.DB, err error) {
	db = LFDB.NewDB(w.RepoSettingPath)
	err = db.Open()
	return db, err
}

//WriteToDB writes a slice of rows to disk.
func (w Winston) WriteToDB(settings pb.RepoSettings, bucket int, t time.Time, rows []LFDB.Row) error {
	repo := LFDB.NewRepo(settings.Repo)
	db, err := repo.CreateBucketIfNotExist(t, w.dataDir, bucket)

	if nil != err {
		log.Error("Failed to create bucket for repo: ", settings.Repo)
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

//Close winston
func (w Winston) Close() error {
	if w.server != nil {
		w.server.GracefulStop()
	}
	return nil
}
