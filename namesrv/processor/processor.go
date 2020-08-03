package processor

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
	. "rocketmq-go/common"
	pb "rocketmq-go/common/proto"
	. "rocketmq-go/logging"
	. "rocketmq-go/namesrv/control"
)

type process func(context.Context, *pb.RemoteCommand) *pb.RemoteCommand

type DefaultProcessor struct {
	Control *Control

	process map[pb.RequestCode]process
}

func NewDefaultProcessor(c *Control) *DefaultProcessor {
	m := make(map[pb.RequestCode]process)
	p := DefaultProcessor{c, m}
	m[pb.RequestCode_PUT_KV_CONFIG] = p.putKVConfig
	m[pb.RequestCode_GET_KV_CONFIG] = p.getKVConfig
	m[pb.RequestCode_DELETE_KV_CONFIG] = p.deleteKVConfig
	m[pb.RequestCode_QUERY_DATA_VERSION] = p.queryBrokerTopicConfig

	m[pb.RequestCode_REGISTER_BROKER] = p.registerBrokerWithFilterServer
	m[pb.RequestCode_UNREGISTER_BROKER] = p.unRegisterBroker
	m[pb.RequestCode_GET_ROUTEINFO_BY_TOPIC] = p.getRouteInfoByTopic
	m[pb.RequestCode_GET_BROKER_CLUSTER_INFO] = p.getBrokerClusterInfo

	m[pb.RequestCode_WIPE_WRITE_PERM_OF_BROKER] = p.wipeWritePermOfBroker
	m[pb.RequestCode_GET_ALL_TOPIC_LIST_FROM_NAMESERVER] = p.getAllTopicListFromNameServer
	m[pb.RequestCode_DELETE_TOPIC_IN_NAMESRV] = p.deleteTopicInNameServer
	m[pb.RequestCode_GET_KVLIST_BY_NAMESPACE] = p.getKVListByNamespace

	m[pb.RequestCode_GET_TOPICS_BY_CLUSTER] = p.getTopicsByCluster
	m[pb.RequestCode_GET_SYSTEM_TOPIC_LIST_FROM_NS] = p.getSystemTopicListFromNs
	m[pb.RequestCode_GET_UNIT_TOPIC_LIST] = p.getUnitTopicList
	m[pb.RequestCode_GET_HAS_UNIT_SUB_TOPIC_LIST] = p.getHasUnitSubTopicList

	m[pb.RequestCode_GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST] = p.getHasUnitSubUnUnitTopicList
	m[pb.RequestCode_UPDATE_NAMESRV_CONFIG] = p.updateConfig
	m[pb.RequestCode_GET_NAMESRV_CONFIG] = p.getConfig
	return &p
}

func (d *DefaultProcessor) Process(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	Log.Debug("receive request",
		zap.Int32("code", request.Code),
		zap.String("addr", GetRemoteAddr(ctx)))

	process, ok := d.process[pb.RequestCode(request.Code)]
	if ok {
		return process(ctx, request)
	}

	Log.Warn("receive request, unknown code", zap.Int32("code", request.Code))
	return nil
}

func checksum(
	ctx context.Context, request *pb.RemoteCommand, header *pb.RegisterBrokerRequestHeader) bool {
	return true
}

func (d *DefaultProcessor) putKVConfig(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.PutKVConfigRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	d.Control.KVConfig.PutKVConfig(reqHeader.Namespace, reqHeader.Key, reqHeader.Value)

	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getKVConfig(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.GetKVConfigRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	value := d.Control.KVConfig.GetKVConfig(reqHeader.Namespace, reqHeader.Key)

	if value != "" {
		respHeader := &pb.GetKVConfigResponseHeader{
			Value: value,
		}
		byteHeader := Serializable(respHeader)
		response.Code = int32(pb.ResponseCode_SUCCESS)
		response.Header = byteHeader
		return response
	}

	response.Code = int32(pb.ResponseCode_QUERY_NOT_FOUND)
	response.Remark = "no config item, namespace: " + reqHeader.Namespace + " key: " + reqHeader.Key
	return response
}

func (d *DefaultProcessor) deleteKVConfig(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.DeleteKVConfigRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	d.Control.KVConfig.DeleteKVConfig(reqHeader.Namespace, reqHeader.Key)

	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) queryBrokerTopicConfig(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.QueryDataVersionRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	return response
}

func (d *DefaultProcessor) registerBrokerWithFilterServer(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.RegisterBrokerRequestHeader{}
	body := &pb.RegisterBrokerBody{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	err = Deserializable(request.Body, body, false)
	if err != nil {
		return nil
	}

	if !checksum(ctx, request, reqHeader) {
		response.Code = int32(pb.ResponseCode_SYSTEM_ERROR)
		response.Remark = "crc32 not match"
		return response
	}

	masterAddr, haServerAddr := d.Control.RouteInfo.RegisterBroker(
		reqHeader.ClusterName,
		reqHeader.BrokerAddr,
		reqHeader.BrokerName,
		reqHeader.BrokerId,
		reqHeader.HaServerAddr,
		&body.FilterServerList)


	respHeader := &pb.RegisterBrokerResponseHeader{
		HaServerAddr: haServerAddr,
		MasterAddr: masterAddr,
	}
	byteHeader := Serializable(respHeader)

	response.Code = int32(pb.ResponseCode_SUCCESS)
	response.Header = byteHeader
	return response
}

func (d *DefaultProcessor) unRegisterBroker(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.UnRegisterBrokerHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	d.Control.RouteInfo.UnRegisterBroker(
		reqHeader.ClusterName,
		reqHeader.BrokerAddr,
		reqHeader.BrokerName,
		reqHeader.BrokerId)

	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getRouteInfoByTopic(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.GetRouteInfoRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	topicRouteData := d.Control.RouteInfo.PickupTopicRouteData(reqHeader.Topic)
	if topicRouteData != nil {
		content, _ := json.Marshal(topicRouteData)
		response.Code = int32(pb.ResponseCode_SUCCESS)
		response.Body = content
		return response
	}

	response.Code = int32(pb.ResponseCode_TOPIC_NOT_EXIST)
	response.Remark = "no topic route info in name server for the topic: " + reqHeader.Topic
	return response
}

func (d *DefaultProcessor) getBrokerClusterInfo(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}

	body := d.Control.RouteInfo.GetAllClusterInfo()

	response.Body = body
	response.Code = int32(pb.ResponseCode_TOPIC_NOT_EXIST)
	return response
}

func (d *DefaultProcessor) wipeWritePermOfBroker(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	//response := &pb.RemoteCommand{}
	//reqHeader := &pb.WipeWritePermOfBrokerRequestHeader{}
	//err := Deserializable(request.Header, reqHeader, false)
	//if err != nil {
	//	return nil
	//}

	return nil
}

func (d *DefaultProcessor) getAllTopicListFromNameServer(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	body := d.Control.RouteInfo.GetAllClusterInfo()

	response.Body = body
	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) deleteTopicInNameServer(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.DeleteTopicInNamesrvRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	d.Control.RouteInfo.DeleteTopic(reqHeader.Topic)

	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getKVListByNamespace(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.GetKVListByNamespaceRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	body := d.Control.KVConfig.GetKVListByNamespace(reqHeader.Namespace)
	if body != nil {
		response.Body = body
		response.Code = int32(pb.ResponseCode_SUCCESS)
		return response
	}

	response.Code = int32(pb.ResponseCode_QUERY_NOT_FOUND)
	response.Remark = "no config item, namespace: " + reqHeader.Namespace
	return response
}

func (d *DefaultProcessor) getTopicsByCluster(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	reqHeader := &pb.GetTopicsByClusterRequestHeader{}
	err := Deserializable(request.Header, reqHeader, false)
	if err != nil {
		return nil
	}

	body := d.Control.RouteInfo.GetTopicByCluster(reqHeader.Cluster)
	response.Body = body
	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getSystemTopicListFromNs(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	body := d.Control.RouteInfo.GetSystemTopicList()

	response.Body = body
	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getUnitTopicList(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	body := d.Control.RouteInfo.GetUnitTopicList()
	response.Body = body
	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getHasUnitSubTopicList(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	body := d.Control.RouteInfo.GetHasUnitSubTopicList()
	response.Body = body
	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) getHasUnitSubUnUnitTopicList(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	response := &pb.RemoteCommand{}
	body := d.Control.RouteInfo.GetHasUnitSubUnUnitTopicList()
	response.Body = body
	response.Code = int32(pb.ResponseCode_SUCCESS)
	return response
}

func (d *DefaultProcessor) updateConfig(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	Log.Sugar().Infof("updateConfig called by %s", GetRemoteAddr(ctx))

	//response := &pb.RemoteCommand{}
	//body := request.Body
	//if body != nil {
	//	bodyStr := string(body)
	//}
	//
	//response.Code = int32(pb.ResponseCode_SUCCESS)
	return nil
}

func (d *DefaultProcessor) getConfig(
	ctx context.Context, request *pb.RemoteCommand) *pb.RemoteCommand {
	return nil
}