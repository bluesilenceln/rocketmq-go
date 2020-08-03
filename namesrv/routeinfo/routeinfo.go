package routeinfo

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"rocketmq-go/common"
	. "rocketmq-go/common/proto/route"
	"rocketmq-go/common/sysflag"
	. "rocketmq-go/logging"
	"sync"
)

const (
	BrokerExpiredTime = 1000 * 5
)

type RouteInfo struct {
	rw sync.RWMutex

	topicQueueTable 	map[string] []QueueData			// map[topic] = Slice[QueueData]
	brokerAddrTable 	map[string] BrokerData			// map[brokerName] = BrokerData
	clusterAddrTable 	map[string] map[string]bool 	// map[clusterName] = map[brokerName]
	brokerLiveTable 	map[string] BrokerLiveInfo		// map[brokerAddr] = BrokerLiveInfo
	filterServerTable 	map[string] []string			// map[brokerAddr] = filterServer
}

func NewRouteInfo() *RouteInfo {
	var lock sync.RWMutex
	return &RouteInfo{
		lock,
		make(map[string][]QueueData, 1024),
		make(map[string]BrokerData, 128),
		make(map[string]map[string]bool, 32),
		make(map[string]BrokerLiveInfo, 256),
		make(map[string][]string, 256),
	}
}

func (r *RouteInfo) deleteTopic(topic string) {
	r.rw.Lock()
	defer r.rw.Unlock()

	delete(r.topicQueueTable, topic)
}

func (r *RouteInfo) ScanNotActiveBroker() {
	Log.Info("scanNotActiveBroker")
	r.rw.Lock()
	defer r.rw.Unlock()

	for addr, info := range r.brokerLiveTable {
		last := info.GetLastUpdateTime()
		now := common.CurrentTimeMills()
		if last + BrokerExpiredTime < now {
			Log.Warn("broker expired",
				zap.String("brokerAddr", addr),
				zap.Int64("lastUpdateTime", last),
				zap.Int64("currentTime", now))
			r.removeBroker(addr)
		} else {
			Log.Debug("broker live", zap.String("brokerAddr", addr))
		}
	}
}

func (r *RouteInfo) removeBroker(brokerAddr string) {
	delete(r.brokerLiveTable, brokerAddr)
	delete(r.filterServerTable, brokerAddr)

	removeBrokerName := false
	var brokerNameFound string
	for brokerName, brokerData := range r.brokerAddrTable {
		for id, addr := range brokerData.BrokerAddrs {
			if brokerAddr == addr {
				brokerNameFound = brokerName
				delete(brokerData.BrokerAddrs, id)
				Log.Info("remove brokerAddr from brokerAddrTable",
					zap.Int64("id", id),
					zap.String("brokerAddr", brokerAddr))
				break
			}
		}

		if len(brokerData.BrokerAddrs) == 0 {
			removeBrokerName = true
			delete(r.brokerAddrTable, brokerName)
			Log.Info("remove brokerName from brokerAddrTable",
				zap.String("brokerName", brokerName))
		}

		if brokerNameFound != "" {
			break
		}
	}

	if brokerNameFound != "" && removeBrokerName {
		for clusterName, brokerNames := range r.clusterAddrTable {
			_, ok := brokerNames[brokerNameFound]
			if ok {
				delete(brokerNames, brokerNameFound)
				Log.Info("remove brokerName, clusterName from clusterAddrTable",
					zap.String("brokerName", brokerNameFound),
					zap.String("clusterName", clusterName))
				if len(brokerNames) == 0 {
					Log.Info("remove clusterName from clusterAddrTable",
						zap.String("clusterName", clusterName))
				}
			}
		}
	}

	if removeBrokerName {

	}
}

func (r *RouteInfo) RegisterBroker(
	clusterName string,
	brokerAddr string,
	brokerName string,
	brokerId int64,
	haServerAddr string,
	filterServerList *[]string) (string, string) {

	r.rw.Lock()
	defer r.rw.Unlock()

	brokerNames, ok := r.clusterAddrTable[clusterName]
	if !ok {
		brokerNames = make(map[string]bool)
		r.clusterAddrTable[clusterName] = brokerNames
	}
	brokerNames[brokerName] = true

	registerFirst := false
	brokerData, ok := r.brokerAddrTable[brokerName]
	if !ok {
		registerFirst = true
		brokerData = BrokerData{Cluster: clusterName, BrokerName: brokerName, BrokerAddrs: make(map[int64]string)}
		r.brokerAddrTable[brokerName] = brokerData
	}

	// Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
	// The same IP:PORT must only have one record in brokerAddrTable
	for id, addr := range brokerData.BrokerAddrs {
		if brokerAddr == addr && brokerId != id {
			delete(brokerData.BrokerAddrs, id)
		}
	}

	_, ok = brokerData.BrokerAddrs[brokerId]
	brokerData.BrokerAddrs[brokerId] = brokerAddr
	registerFirst = registerFirst || (ok == false)

	prevBrokerLiveInfo := NewBrokerLiveInfo(common.CurrentTimeMills(), haServerAddr)
	r.brokerLiveTable[brokerAddr] = *prevBrokerLiveInfo
	Log.Info("new broker registered",
		zap.String("brokerAddr", brokerAddr),
		zap.String("haServerAddr", haServerAddr))

	if filterServerList != nil {

	}

	if brokerId != 0 {
		masterAddr, ok := brokerData.BrokerAddrs[0]
		if ok {
			brokerLiveInfo, ok := r.brokerLiveTable[masterAddr]
			if ok {
				return masterAddr, brokerLiveInfo.haServerAddr
			}
		}
	}

	return "", ""
}

func (r *RouteInfo) UnRegisterBroker(clusterName string, brokerAddr string, brokerName string, brokerId int64) {
	r.rw.Lock()
	defer r.rw.Unlock()

	_, ok := r.brokerLiveTable[brokerAddr]
	if ok {
		delete(r.brokerLiveTable, brokerAddr)
		Log.Info("unregisterBroker, remove from brokerLiveTable OK",
			zap.String("brokerAddr", brokerAddr))
	} else {
		Log.Info("unregisterBroker, remove from brokerLiveTable Failed",
			zap.String("brokerAddr", brokerAddr))
	}

	delete(r.filterServerTable, brokerAddr)

	removeBrokerName := false
	brokerData, ok := r.brokerAddrTable[brokerName]
	if ok {
		_, ok = brokerData.BrokerAddrs[brokerId]
		fmt.Println(len(brokerData.BrokerAddrs))
		if ok {
			delete(brokerData.BrokerAddrs, brokerId)
			Log.Info("unregisterBroker, remove from BrokerAddrs OK",
				zap.Int64("brokerId", brokerId),
				zap.String("brokerAddr", brokerAddr))
		} else {
			Log.Info("unregisterBroker, remove from BrokerAddrs Failed",
				zap.Int64("brokerId", brokerId),
				zap.String("brokerAddr", brokerAddr))
		}

		if len(brokerData.BrokerAddrs) == 0 {
			delete(r.brokerAddrTable, brokerName)
			Log.Info("unregisterBroker, remove name from brokerAddrTable OK",
				zap.String("brokerName", brokerName))
		}
		removeBrokerName = true
	}

	if removeBrokerName {
		nameSet, ok := r.clusterAddrTable[clusterName]
		if ok {
			_, ok = nameSet[brokerName]
			if ok {
				delete(nameSet, brokerName)
				Log.Info("unregisterBroker, remove name from clusterAddrTable OK",
					zap.String("brokerName", brokerName))
			} else {
				Log.Info("unregisterBroker, remove name from clusterAddrTable Failed",
					zap.String("brokerName", brokerName))
			}

			if len(nameSet) == 0 {
				delete(r.clusterAddrTable, clusterName)
				Log.Info("unregisterBroker, remove cluster from clusterAddrTable",
					zap.String("clusterName", clusterName))
			}
			//this.removeTopicByBrokerName(brokerName);
		}
	}
}

func (r *RouteInfo) PickupTopicRouteData(topic string) *TopicRouteData {
	foundQueueData := false
	foundBrokerData := false
	brokerNameSet := make(map[string]bool)
	brokerDataList := make([]BrokerData, 0)
	filterServerMap := make(map[string][]string)
	topicRouteData := TopicRouteData{}

	topicRouteData.SetBrokerDataList(&brokerDataList)
	topicRouteData.SetFilterServerTable(&filterServerMap)

	r.rw.RLock()
	defer r.rw.RUnlock()

	queueDataList, ok := r.topicQueueTable[topic]
	if ok {
		topicRouteData.SetQueueDataList(&queueDataList)
		foundQueueData = true

		for _, qd := range queueDataList {
			brokerNameSet[qd.BrokerName] = true
		}

		for brokerName := range brokerNameSet {
			brokerData, ok := r.brokerAddrTable[brokerName]
			if ok {
				brokerDataClone := BrokerData{Cluster: brokerData.Cluster, BrokerName: brokerData.BrokerName, BrokerAddrs: brokerData.BrokerAddrs}
				brokerDataList = append(brokerDataList, brokerDataClone)
				foundBrokerData = true

				for _, brokerAddr := range brokerDataClone.BrokerAddrs {
					filterServerList := r.filterServerTable[brokerAddr]
					filterServerMap[brokerAddr] = filterServerList
				}
			}
		}
	}

	Log.Sugar().Debug("PickupTopicRouteData topic: %s, topicRouteData: %v", topic, topicRouteData)

	if foundQueueData && foundBrokerData {
		return &topicRouteData
	}

	return nil
}

func (r *RouteInfo) GetAllClusterInfo() []byte {
	r.rw.RLock()
	defer r.rw.RUnlock()

	c := ClusterInfo{
		BrokerAddrTable:  r.brokerAddrTable,
		ClusterAddrTable: r.clusterAddrTable,
	}

	data, _ := json.Marshal(c)
	return data
}

func (r *RouteInfo) GetAllTopicList() []byte {
	r.rw.RLock()
	defer r.rw.RUnlock()

	topicList := TopicList{TopicList: make(map[string]bool)}

	for topic := range r.topicQueueTable {
		topicList.TopicList[topic] = true
	}

	data, _ := json.Marshal(topicList)
	return data
}

func (r *RouteInfo) DeleteTopic(topic string) {
	r.rw.Lock()
	defer r.rw.Unlock()

	delete(r.topicQueueTable, topic)
}

func (r *RouteInfo) GetTopicByCluster(cluster string) []byte {
	r.rw.RLock()
	r.rw.Unlock()

	topicList := TopicList{TopicList: make(map[string]bool)}
	brokerSet, _ := r.clusterAddrTable[cluster]
	for brokerName := range brokerSet {
		for topic := range r.topicQueueTable {
			queueDataList := r.topicQueueTable[topic]
			for i := 0; i < len(queueDataList); i++ {
				if brokerName == queueDataList[i].BrokerName {
					topicList.TopicList[topic] = true
					break
				}
			}
		}
	}

	data, err := json.Marshal(topicList)
	if err == nil {
		return data
	}

	return nil
}

func (r *RouteInfo) GetSystemTopicList() []byte {
	r.rw.RLock()
	defer r.rw.Unlock()

	topicSet := make(map[string]bool)
	topicList := TopicList{
		TopicList: topicSet,
	}

	for cluster, brokerSet := range r.clusterAddrTable {
		topicList.TopicList[cluster] = true
		for broker := range brokerSet {
			topicList.TopicList[broker] = true
		}
	}

	for _, brokerData := range r.brokerAddrTable {
		brokerAddrList := brokerData.BrokerAddrs
		for _, brokerAddr := range brokerAddrList {
			topicList.BrokerAddr = brokerAddr
		}
	}

	data, err := json.Marshal(topicList)
	if err == nil {
		return data
	}

	return nil
}

func (r *RouteInfo) GetUnitTopicList() []byte {
	r.rw.RLock()
	defer r.rw.Unlock()

	topicSet := make(map[string]bool)
	topicList := TopicList{
		TopicList: topicSet,
	}

	for topic, queueData := range r.topicQueueTable {
		if len(queueData) > 0 && sysflag.HasUnitFlag(queueData[0].TopicSysFlag) {
			topicSet[topic] = true
		}
	}

	data, err := json.Marshal(topicList)
	if err == nil {
		return data
	}

	return nil
}

func (r *RouteInfo) GetHasUnitSubTopicList() []byte {
	r.rw.RLock()
	defer r.rw.Unlock()

	topicSet := make(map[string]bool)
	topicList := TopicList{
		TopicList: topicSet,
	}

	for topic, queueData := range r.topicQueueTable {
		if len(queueData) > 0 && sysflag.HasUnitSubFlag(queueData[0].TopicSysFlag) {
			topicSet[topic] = true
		}
	}

	data, err := json.Marshal(topicList)
	if err == nil {
		return data
	}

	return nil
}

func (r *RouteInfo) GetHasUnitSubUnUnitTopicList() []byte {
	r.rw.RLock()
	defer r.rw.Unlock()

	topicSet := make(map[string]bool)
	topicList := TopicList{
		TopicList: topicSet,
	}

	for topic, queueData := range r.topicQueueTable {
		if len(queueData) > 0 &&
			!sysflag.HasUnitFlag(queueData[0].TopicSysFlag) &&
			sysflag.HasUnitSubFlag(queueData[0].TopicSysFlag) {
			topicSet[topic] = true
		}
	}

	data, err := json.Marshal(topicList)
	if err == nil {
		return data
	}

	return nil
}

