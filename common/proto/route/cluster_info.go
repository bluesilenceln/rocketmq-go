package common

type ClusterInfo struct {
	BrokerAddrTable map[string]BrokerData
	ClusterAddrTable map[string]map[string]bool
}
