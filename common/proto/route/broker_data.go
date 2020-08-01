package common

type BrokerData struct {
	Cluster string
	BrokerName string
	BrokerAddrs map[int64]string
}