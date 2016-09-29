package server

import (
	"fmt"
	LFDB "github.com/LogRhythm/Winston/lfdb"
	pb "github.com/LogRhythm/Winston/pb"
	TIME "github.com/LogRhythm/Winston/time"
	log "github.com/cihub/seelog"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"net"
	"os"
	"stablelib.com/v1/crypto/siphash"
	"sync"
	"time"
)

//Winston
type Winston struct {
	server          *grpc.Server
	listen          net.Listener
	dataDir         string
	RepoSettingPath string
	repos           *Repos
}

type Repos struct {
	R map[string]*LFDB.Repo
	*sync.Mutex
}

const SETTINGS_BUCKET = "v1_settings"

//NewWinston creates the winston server
func NewWinston(conf WinstonConf) Winston {
	grpc.EnableTracing = true
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", conf.Port))
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	log.Info("Winston listening on: ", lis.Addr().String())
	var opts []grpc.ServerOption
	// if *tls {
	// 	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	// 	if err != nil {
	// 		grpclog.Fatalf("Failed to generate credentials %v", err)
	// 	}
	// 	opts = []grpc.ServerOption{grpc.Creds(creds)}
	// }
	w := Winston{
		server: grpc.NewServer(opts...),
		listen: lis, dataDir: conf.DataDir,
		repos: &Repos{R: make(map[string]*LFDB.Repo, 0), Mutex: &sync.Mutex{}},
	}

	if _, err := os.Stat(w.dataDir); os.IsNotExist(err) {
		err := os.Mkdir(w.dataDir, 0600)
		if err != nil {
			panic(fmt.Sprintf("failed to make DATA_DIR: %s for winston %v", w.dataDir, err))
		}
	}
	if len(w.dataDir) == 0 {
		panic("No DATA_DIR set in your .env file")
	}
	w.RepoSettingPath = fmt.Sprintf("%s/settings%s", w.dataDir, LFDB.DB_EXT)
	pb.RegisterV1Server(w.server, w)
	return w
}

//Start ...
func (w Winston) Start() {
	go w.server.Serve(w.listen)
}

//DeleteRepo removes the repository from the file system and from the settings
func (w Winston) DeleteRepo(repo string) error {
	return fmt.Errorf("Not implemented")
}

type RowsBucketedByBucketAndDate []map[time.Time][]LFDB.Row

const (
	MAX_BUCKET_SIZE = 1024
)

//ERR_INVALID_REPO_NAME ...
var ERR_INVALID_REPO_NAME = fmt.Errorf("empty repo name")

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
		if err != nil {
			log.Error("transaction: ", err)
			return err
		}
		if len(in.Repo) == 0 {
			return ERR_INVALID_REPO_NAME
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
				t = TIME.MSToTime(int64(r.TimeMs))
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
			//truncate to day
			mapTime := TIME.TimeRoundToDay(t)

			if bucketRows[bucket][mapTime] == nil {
				bucketRows[bucket][mapTime] = make([]LFDB.Row, 0)
			}
			bucketRows[bucket][mapTime] = append(bucketRows[bucket][mapTime], LFDB.Row{Time: t, Data: r.Data})
		}
	}
}

//GetBuckets returns you a stream of buckets for a single repository
func (w Winston) GetBuckets(req *pb.BucketsRequest, stream pb.V1_GetBucketsServer) error {
	settings, err := w.getSettingsForRepo(req.Repo)
	if err != nil {
		return fmt.Errorf("failed to get settings for repo: ", err)
	}
	repo := w.GetRepo(*settings)
	startTime := TIME.TimeRoundToDay(TIME.MSToTime(int64(req.StartTimeMs)))
	endTime := TIME.TimeRoundToDay(TIME.MSToTime(int64(req.EndTimeMs)))
	repo.GetPartitionsCallFunc(startTime, endTime, func(path string) error {
		return stream.Send(&pb.Bucket{Path: path})
	})
	return nil
}

const ReadBatchSize = 1000

func (w Winston) ReadByTime(read *pb.Read, stream pb.V1_ReadByTimeServer) error {
	return fmt.Errorf("not implemented")
}

func (w Winston) ReadBucketByTime(read *pb.ReadBucket, stream pb.V1_ReadBucketByTimeServer) error {
	if read == nil {
		return fmt.Errorf("invalid read request")
	}
	settings, err := w.getSettingsForRepo(read.Repo)
	if err != nil {
		return fmt.Errorf("failed to get settings: ", err)
	}
	count := 0
	repo := w.GetRepo(*settings)
	log.Info("Read Request: ", read)
	startTime := TIME.MSToTime(int64(read.StartTimeMs))
	endTime := TIME.MSToTime(int64(read.EndTimeMs))
	err = repo.ReadPartitionByTime(read.BucketPath, ReadBatchSize, startTime.UnixNano(), endTime.UnixNano(), func(rows []*pb.Row) error {
		if len(rows) > 0 {
			count += len(rows)
			msg := &pb.ReadResponse{Repo: settings.Repo, Rows: rows}
			err = stream.Send(msg)
			if err != nil {
				log.Error("send to client: ", err)
				return err
			}
		}
		return err
	})
	log.Info("repo: ", settings.Repo, " Read ", count, " records")
	return err

}

func (w Winston) getSettingsForRepo(repo string) (settings *pb.RepoSettings, err error) {
	settings = &pb.RepoSettings{}
	db, err := w.openSettingsDBReadOnly()
	defer db.Close()
	if err != nil {
		log.Error("Failed to open settings db: ", err)
		return settings, err
	}
	bytes, err := db.ReadKey(repo, SETTINGS_BUCKET)
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
	log.Info("updating settings for repo: ", settings.Repo)
	err = db.WriteKey(settings.Repo, data, SETTINGS_BUCKET)
	return &pb.EMPTY{}, err
}

func (w Winston) GetRepo(settings pb.RepoSettings) *LFDB.Repo {
	w.repos.Lock()
	defer w.repos.Unlock()
	r, ok := w.repos.R[settings.Repo]
	if !ok {
		repo := LFDB.NewRepo(settings.Repo, w.dataDir)
		r = &repo
		w.repos.R[settings.Repo] = r

	}
	return r
}

func (w Winston) openSettingsDB() (db LFDB.DB, err error) {
	db = LFDB.NewDB(w.RepoSettingPath)
	err = db.Open()
	return db, err
}

//openSettingsDBReadOnly will open  the db using the read only methods in boltdb
func (w Winston) openSettingsDBReadOnly() (db LFDB.DB, err error) {
	db = LFDB.NewDB(w.RepoSettingPath)
	err = db.OpenReadOnly()
	return db, err
}

//WriteToDB writes a slice of rows to disk.
func (w Winston) WriteToDB(settings pb.RepoSettings, bucket int, t time.Time, rows []LFDB.Row) error {
	repo := w.GetRepo(settings)
	db, err := repo.CreatePartitionIfNotExist(t, w.dataDir, bucket)
	if err != nil {
		log.Error("failed to create bucket for repo: ", settings.Repo)
		return err
	}
	defer repo.ClosePartition(db.Path)
	return db.WriteBatch(rows...)
}

//Close winston
func (w Winston) Close() error {
	if w.server != nil {
		w.server.GracefulStop()
	}
	return nil
}
