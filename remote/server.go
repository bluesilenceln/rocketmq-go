package remote

import (
	"context"
	//"github.com/golang/protobuf/proto"
	//"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"io"
	"net"
	pb "rocketmq-go/common/proto"
	. "rocketmq-go/logging"
)

type Processor func(context.Context, *pb.RemoteCommand) *pb.RemoteCommand

type Server struct {
	Processor

	addr string
	srv *grpc.Server
	pb *pb.UnimplementedRemoteRPCServer
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

func (s *Server) Start() {
	listen, err := net.Listen("tcp", s.addr)
	if err != nil {
		Log.Sugar().Fatalf("Failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	//opts = append(opts, grpc.UnaryInterceptor(interceptor))
	opts = append(opts, grpc.StatsHandler(s))

	s.srv = grpc.NewServer(opts...)
	pb.RegisterRemoteRPCServer(s.srv, s)

	go func() {
		err = s.srv.Serve(listen)
		if err != nil {
			Log.Sugar().Infof("Remote server exit: %v", err)
		}
	}()
}

func (s *Server) Stop() {
	s.srv.Stop()
	Log.Info("Remote server stopped")
}

func (s *Server) Process(stream pb.RemoteRPC_ProcessServer) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		resp := s.Processor(ctx, req)
		if resp == nil {
			continue
		}

		if err = stream.Send(resp); err != nil {
			return nil
		}
	}
}

func (s *Server) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (s *Server) HandleRPC(ctx context.Context, stats stats.RPCStats) {
}

func (s *Server) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, "conn", info)
}

func (s *Server) HandleConn(ctx context.Context, state stats.ConnStats) {
	info, ok := ctx.Value("conn").(*stats.ConnTagInfo)
	if !ok {
		Log.Error("Can't find connection info")
		return
	}

	addr := info.RemoteAddr.String()

	switch state.(type) {
	case *stats.ConnBegin:
		Log.Sugar().Infof("Connected, addr: %s", addr)
	case *stats.ConnEnd:
		Log.Sugar().Infof("Closed, addr: %s", info.RemoteAddr.String())
	default:
		Log.Sugar().Errorf("Unknown type: %#v", info)
	}
}
