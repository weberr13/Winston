package main

import (
	"fmt"
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	"github.com/vrecan/death"
	// "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"io"
	"net"
	SYS "syscall"
)

func main() {
	defer log.Flush()
	logger, err := log.LoggerFromConfigAsFile("seelog.xml")

	if err != nil {
		log.Warn("failed to load seelog config", err)
	}

	log.ReplaceLogger(logger)
	log.Info("starting winston")

	death := death.NewDeath(SYS.SIGINT, SYS.SIGTERM)
	grpc.EnableTracing = true
	// flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 5001))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	// if *tls {
	// 	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	// 	if err != nil {
	// 		grpclog.Fatalf("Failed to generate credentials %v", err)
	// 	}
	// 	opts = []grpc.ServerOption{grpc.Creds(creds)}
	// }
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterInsertServer(grpcServer, newServer())
	go grpcServer.Serve(lis)
	death.WaitForDeath()
	log.Info("shutdown")
}

type InsertServer struct {
}

func newServer() *InsertServer {
	return &InsertServer{}
}

func (i InsertServer) Push(stream pb.Insert_PushServer) error {
	cnt := 0
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			// log.Info("Done")
			return nil //done with stream
		}
		if err != nil {
			log.Error("failed connection")
			return err
		}
		cnt++
		if cnt%10000 == 0 {
			log.Info("PUSHING: ", cnt, " msg: ", in)
		}
	}

}
