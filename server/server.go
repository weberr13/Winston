package server

import (
	pb "github.com/LogRhythm/Winston/pb"
	log "github.com/cihub/seelog"
	// "golang.org/x/net/context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"io"
	"net"
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
	w := Winston{server: grpc.NewServer(opts...), listen: lis}
	pb.RegisterV1Server(w.server, w)
	return w
}

func (w Winston) Start() {
	go w.server.Serve(w.listen)
}

func (w Winston) Push(stream pb.V1_PushServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			// log.Info("Done")
			return nil //done with stream
		}
		if err != nil {
			log.Error("failed transaction: ", err)
			return err
		}
	}

}

func (w Winston) Close() error {
	if w.server != nil {
		w.server.GracefulStop()
	}
	return nil
}
