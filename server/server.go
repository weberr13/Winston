package server

import (
	"encoding/binary"
	LFDB "github.com/LogRhythm/Winston/lfdb"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	ENV "github.com/joho/godotenv"
	// "github.com/davecgh/go-spew/spew"
	"fmt"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"stablelib.com/v1/crypto/siphash"
	// "google.golang.org/grpc/grpclog"
	SNAP "github.com/LogRhythm/Winston/snappy"
	"github.com/boltdb/bolt"
	"io"
	"net"
	// "runtime"
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

const MAX_BUCKET_SIZE = 1024

//Push data into winston db.
func (w Winston) Write(stream pb.V1_WriteServer) error {
	var settings *pb.RepoSettings
	bucketRows := make(RowsBucketedByBucketAndDate, MAX_BUCKET_SIZE)

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
			if settings.GroupByBuckets != 0 {
				var value []byte
				for _, field := range settings.GroupByFields {
					value = append(value, []byte(gjson.Get(string(r.Data), field).String())...)
				}
				hash := siphash.Hash(0, MAX_BUCKET_SIZE, value)
				bucket = hash % uint64(settings.GroupByBuckets)
			}
			var t time.Time
			if r.TimeMs != 0 {
				t = msToTime(int64(r.TimeMs))
			} else if len(settings.TimeField) != 0 && r.TimeMs == 0 {

				timeValue := gjson.Get(string(r.Data), settings.TimeField).String()
				t, err = time.Parse(time.RFC3339, timeValue)
				if err != nil {
					log.Error("invalid timefield: ", settings.TimeField, " for repo: ", settings.Repo, " error: ", err)
					return fmt.Errorf("invalid time: ", timeValue, " in timefield: ", settings.TimeField, " error: ", err)
				}
			} else {
				//if no time set assume it's now
				t = time.Now()
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

const (
	millisPerSecond     = int64(time.Second / time.Millisecond)
	nanosPerMillisecond = int64(time.Millisecond / time.Nanosecond)
)

func msToTime(msTime int64) time.Time {
	return time.Unix(msTime/millisPerSecond,
		(msTime%millisPerSecond)*nanosPerMillisecond)
}

const ReadBatchSize = 300

func (w Winston) ReadByTime(read *pb.Read, stream pb.V1_ReadByTimeServer) error {
	return fmt.Errorf("not implemented")
}

func (w Winston) ReadBucketByTime(pull *pb.ReadBucket, stream pb.V1_ReadBucketByTimeServer) error {
	if pull == nil {
		return fmt.Errorf("invalid request")
	}
	settings, err := w.getSettingsForRepo(pull.Repo)
	if err != nil {
		return fmt.Errorf("failed to get settings: ", err)
	}
	count := 0
	repo := LFDB.NewRepo(settings.Repo)
	err = repo.ReadBucket(fmt.Sprintf("%s/%s/%s", w.dataDir, pull.Repo, pull.BucketPath), func(tx *bolt.Tx) error {
		rows := make([]*pb.Row, 0, ReadBatchSize)
		b := tx.Bucket([]byte("data"))

		if b == nil {
			return fmt.Errorf("data bucket does not exist")
		}
		tb := b.Bucket([]byte("time"))
		if nil == tb {
			return fmt.Errorf("time bucket doesn't exist")
		}

		tc := tb.Cursor()
		bc := b.Cursor()
		startTime := msToTime(int64(pull.StartTimeMs)).UnixNano()
		endTime := msToTime(int64(pull.EndTimeMs)).UnixNano()
		for k, v := tc.First(); k != nil; k, v = tc.Next() {

			rt := binary.BigEndian.Uint64(v)

			if rt >= uint64(startTime) && rt <= uint64(endTime) {
				_, bv := bc.Seek(k)
				if bv == nil {
					log.Error("key: ", k, " repo: ", repo.Name)
					continue
				}
				bv, err = SNAP.DecompressBytes(bv)
				if err != nil {
					return err
				}
				rows = append(rows, &pb.Row{TimeMs: rt, Data: bv})
				if len(rows) >= ReadBatchSize {
					err = stream.Send(&pb.ReadResponse{Repo: settings.Repo, Rows: rows})
					if err != nil {
						log.Error("send to client: ", err)
						return err
					}
					// log.Info("foreach Rows: ", len(rows), " flushing")
					rows = make([]*pb.Row, 0, ReadBatchSize)
				}
				count++
			}
		}
		if len(rows) > 0 {
			log.Info("rows: ", len(rows), " flushing")
			msg := &pb.ReadResponse{Repo: settings.Repo, Rows: rows}
			err = stream.Send(msg)
			if err != nil {
				log.Error("send to client: ", err)
			}
		}
		return nil
	})

	log.Info("repo: ", settings.Repo, " Read ", count, " records")
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
		log.Error("failed to open settings db: ", err)
		return nil, err
	}
	if settings == nil || len(settings.Repo) == 0 {
		return nil, fmt.Errorf("failed to set repo name")
	}
	if settings.GroupByBuckets > MAX_BUCKET_SIZE {
		return nil, fmt.Errorf("bucket size set to greater then 1024")
	}
	data, err := settings.Marshal()
	if nil != err {
		log.Error("failed to marshal conf: ", err)
		return nil, err
	}
	log.Info("updating settings for repo: ", settings.Repo, " with values: ", string(data))
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
		log.Error("failed to create bucket for repo: ", settings.Repo)
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
