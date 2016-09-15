package server

import (
	"fmt"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	uuid "github.com/satori/go.uuid"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const TEMP_PREFIX = "winston_test"

func TestWinstonServerBasic(t *testing.T) {
	DeleteOldTmpFiles()
	defer log.Flush()
	Convey("conf doesn't panic", t, func() {
		So(func() { NewWinstonConf() }, ShouldNotPanic)
	})

	Convey("Init", t, func() {
		c := TempConf()
		w := NewWinston(c)
		w.Start()
		So(w.Close(), ShouldBeNil)
	})

	Convey("Open settings and close it", t, func() {
		c := TempConf()
		w := NewWinston(c)
		defer w.Close()
		w.Start()
		settings, err := w.openSettingsDB()
		So(err, ShouldBeNil)
		defer settings.Close()
	})

	Convey("Open Read only doesn't exist, should fail", t, func() {
		c := TempConf()
		w := NewWinston(c)
		defer w.Close()
		w.Start()
		settings, err := w.openSettingsDBReadOnly()
		defer settings.Close()
		So(err, ShouldNotBeNil)
	})
}

func TestWinstonServerUpsertRepo(t *testing.T) {
	defer log.Flush()
	Convey("Create new repo and validate with read", t, func() {
		c := TempConf()
		w := NewWinston(c)
		w.Start()
		defer w.Close()
		con, client := NewConnAndClient(c.Port)
		defer con.Close()
		set := &pb.RepoSettings{
			Repo:           "FAKE_REPO_THINGY",
			Format:         pb.RepoSettings_JSON,
			TimeField:      "normalDate",
			GroupByFields:  []string{0: "id"},
			GroupByBuckets: 8}
		err := UpsertWithTimeout(2*time.Second, client, set)
		So(err, ShouldBeNil)
		settings, err := w.getSettingsForRepo(set.Repo)
		So(err, ShouldBeNil)
		So(settings, ShouldResemble, set)
	})

	Convey("Create new repo and then update it validate new data", t, func() {
		c := TempConf()
		w := NewWinston(c)
		w.Start()
		defer w.Close()
		con, client := NewConnAndClient(c.Port)
		defer con.Close()
		set := &pb.RepoSettings{
			Repo:           "FAKE_REPO_THINGY",
			Format:         pb.RepoSettings_JSON,
			TimeField:      "normalDate",
			GroupByFields:  []string{0: "id"},
			GroupByBuckets: 8}
		err := UpsertWithTimeout(2*time.Second, client, set)
		So(err, ShouldBeNil)
		settings, err := w.getSettingsForRepo(set.Repo)
		So(err, ShouldBeNil)
		So(settings, ShouldResemble, set)

		//update the bucket count validate we get the new count
		set.GroupByBuckets = 16
		err = UpsertWithTimeout(2*time.Second, client, set)
		So(err, ShouldBeNil)
		settings, err = w.getSettingsForRepo(set.Repo)
		So(err, ShouldBeNil)
		So(settings, ShouldResemble, set)
	})
}

func TestWinstonServerWrite(t *testing.T) {
	defer log.Flush()
	WRITE_REPO := "Write_Test"
	c := TempConf()
	w := NewWinston(c)
	w.Start()
	defer w.Close()
	con, client := NewConnAndClient(c.Port)
	defer con.Close()
	Convey("Setup Repo", t, func() {
		set := &pb.RepoSettings{
			Repo:           WRITE_REPO,
			Format:         pb.RepoSettings_JSON,
			TimeField:      "normalDate",
			GroupByFields:  []string{0: "id"},
			GroupByBuckets: 8}
		err := UpsertWithTimeout(2*time.Second, client, set)
		So(err, ShouldBeNil)

		Convey("Write records", func() {
			stream, err := client.Write(context.Background())
			if err != nil {
				log.Error("Failed to push stream to context background: ", err)
				return
			}
			//write a few records, has to be enough that every bucket has a record
			data := MakeRequests(10, 20, WRITE_REPO)
			for _, note := range data {
				err := stream.Send(note)
				So(err, ShouldBeNil)
			}
			_, err = stream.CloseAndRecv()
			So(err, ShouldBeNil)
		})
		Convey("Get Buckets and Read Records", func() {
			buckets := getBuckets(client, WRITE_REPO)
			So(len(buckets), ShouldEqual, 8)
			responseBuckets, err := readAllBuckets(buckets, client, WRITE_REPO, time.Now().Add(-1*time.Minute), time.Now())
			So(err, ShouldBeNil)
			So(len(responseBuckets), ShouldEqual, 8)
			for _, b := range responseBuckets {
				So(len(b), ShouldEqual, 1)
				So(len(b[0].Rows), ShouldBeBetweenOrEqual, 15, 35)
			}
		})
	})

}

func MakeRequests(cnt int, batchCnt int, repo string) []*pb.WriteRequest {
	data := make([]*pb.WriteRequest, 0)

	for i := 0; i < cnt; i++ {
		pr := &pb.WriteRequest{
			Repo: repo,
		}
		for bc := 0; bc < batchCnt; bc++ {
			row := &pb.Row{
				Data: []byte(fmt.Sprintf("{ \"id\": \"%s\", \"normalDate\": \"%s\",  \"woo\" : \"this thing\", \"is\": \"a bunch of extra data that we ddin't have in the thingy magiger bob before. But we do now don't we bob.\"}", uuid.NewV4(), time.Now().Format(time.RFC3339))),
			}
			pr.Rows = append(pr.Rows, row)
		}
		data = append(data, pr)
	}
	return data
}

func UpsertWithTimeout(timeout time.Duration, client pb.V1Client, settings *pb.RepoSettings) (err error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("Timeout trying to connect to server: %v", err)
		default:
			_, err = client.UpsertRepo(context.Background(), settings)
			log.Info(err)
			if err == nil {
				return nil
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
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
		log.Info(f)
		err := os.RemoveAll(f)
		if err != nil {
			log.Error("Failed to remove temp dir: ", f, " error ", err)
		}
	}

}

func getBuckets(client pb.V1Client, repo string) []string {
	stream, err := client.GetBuckets(context.Background(), &pb.BucketsRequest{
		Repo:        repo,
		StartTimeMs: uint64(time.Now().Add(-72*time.Hour).UnixNano() / int64(time.Millisecond)),
		EndTimeMs:   uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	})
	if err != nil {
		panic(fmt.Sprintf("get buckets: %v", err))
	}
	buckets := []string{}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			log.Info("Finished getting buckets")
			return buckets
		}
		buckets = append(buckets, in.Path)

	}
	return buckets
}

func readAllBuckets(buckets []string, client pb.V1Client, repo string, startTime time.Time, endTime time.Time) (bucketResponses map[string][]*pb.ReadResponse, err error) {
	t := time.Now()
	bucketResponses = make(map[string][]*pb.ReadResponse, 0)
	for _, b := range buckets {
		bucketResponses[b] = make([]*pb.ReadResponse, 0)
		pull := pb.ReadBucket{
			Repo:        repo,
			StartTimeMs: uint64(startTime.UnixNano() / int64(time.Millisecond)),
			EndTimeMs:   uint64(endTime.UnixNano() / int64(time.Millisecond)),
			BucketPath:  b,
		}
		stream, err := client.ReadBucketByTime(context.Background(), &pull)
		if err != nil {
			return bucketResponses, err
		}
		cnt := 0
		rcds := 0
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				duration := time.Since(t)
				log.Info("bucket: ", b, " records: ", rcds, " ", duration)
				break
			}
			if err != nil {
				log.Error("Failed to read row from winston: ", err, " resp: ", resp)
				return bucketResponses, err
			}
			bucketResponses[b] = append(bucketResponses[b], resp)
			rcds += len(resp.Rows)
			cnt++
		}
	}
	return bucketResponses, err
}

func TempConf() WinstonConf {
	dir, err := ioutil.TempDir("", "winston_test")
	if err != nil {
		panic(fmt.Sprintf("Failed to get winston conf for test: %v", err))
	}
	c := WinstonConf{DataDir: dir, Port: 5777} //0 port makes it random available port
	return c

}

func NewConnAndClient(port int) (*grpc.ClientConn, pb.V1Client) {
	conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithInsecure())
	if err != nil {
		panic("Failed to create network grpc client connection!")
	}
	client := pb.NewV1Client(conn)
	return conn, client
}
