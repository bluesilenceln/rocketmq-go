package remote

import (
	"context"
	"google.golang.org/grpc"
	"io"
	"log"
	pb "rocketmq-go/common/proto"
)

type Client struct {
	addr string

	cli pb.RemoteRPCClient

	sendChan chan *pb.RemoteCommand
	recvChan chan *pb.RemoteCommand
}

func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
		sendChan: make(chan *pb.RemoteCommand, 10),
		recvChan: make(chan *pb.RemoteCommand, 10),
	}
}

func (c *Client) Start() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	c.cli = pb.NewRemoteRPCClient(conn)
	c.process()
}

func (c *Client) process() {
	stream, err := c.cli.Process(context.Background())
	if err != nil {
		log.Fatalf("%v.RouteChat(_) = _, %v", c.cli, err)
	}
	exitChan := make(chan struct{})
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					close(exitChan)
					return
				}
				log.Printf("Failed to receive a response: %v", err)
				return
			}
			c.recvChan <- resp
		}
	}()

	go func() {
		for {
			req := <- c.sendChan
			if err := stream.Send(req); err != nil {
				if err == io.EOF {
					close(exitChan)
					return
				} else {
					log.Printf("Fail to request: %v", err)
					return
				}

			}
		}
	}()

	<-exitChan
	_ = stream.CloseSend()
}

func (c *Client) Send(request *pb.RemoteCommand) {
	c.sendChan <- request
}

func (c *Client) Recv() *pb.RemoteCommand {
	response := <- c.recvChan
	return response
}