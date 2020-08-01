package common

type TopicRouteData struct {
	orderTopicConf string
	queueDataList *[]QueueData
	brokerDataList *[]BrokerData
	filterServerTable *map[string][]string
}

func (t *TopicRouteData) SetBrokerDataList(l *[]BrokerData) {
	t.brokerDataList = l
}

func (t *TopicRouteData) GetBrokerDataList() *[]BrokerData {
	return t.brokerDataList
}

func (t *TopicRouteData) SetFilterServerTable(f *map[string][]string) {
	t.filterServerTable = f
}

func (t *TopicRouteData) SetQueueDataList(l *[]QueueData) {
	t.queueDataList = l
}

