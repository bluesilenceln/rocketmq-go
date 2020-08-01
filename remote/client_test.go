package remote

import (
	"log"
	"rocketmq-go/common"
	pb "rocketmq-go/common/proto"
	"testing"
)

func TestRegisterBrokerHeader(t *testing.T) {
	c := NewClient("0.0.0.0:9876")
	go c.Start()

	header := &pb.RegisterBrokerRequestHeader{
		BrokerName: "broker-a",
		BrokerAddr: "192.168.1.1:8675",
		ClusterName: "cluster",
		BrokerId: 0,
		Compressed: false,
		BodyCrc32: 0,
	}
	request := &pb.RemoteCommand{
		Code: int32(pb.RequestCode_REGISTER_BROKER),
		Header: common.Serializable(header),
	}
	c.Send(request)
	response := c.Recv()
	log.Printf("registerBroker, code: %d, remark: %s", response.Code, response.Remark)
}

//func TestUnRegisterBrokerHeader(t *testing.T) {
//	c := NewClient("0.0.0.0:9876")
//	go c.Start()
//
//	header := &pb.UnRegisterBrokerHeader{
//		BrokerName: "broker-a",
//		BrokerAddr: "192.168.1.1:8675",
//		ClusterName: "cluster",
//		BrokerId: 0,
//	}
//	request := &pb.RemoteCommand{
//		Code: int32(pb.RequestCode_UNREGISTER_BROKER),
//		Header: remote.SerializableM(header),
//	}
//	c.Send(request)
//	response := c.Recv()
//	log.Printf("unregisterBroker, code: %d, remark: %s", response.Code, response.Remark)
//}

