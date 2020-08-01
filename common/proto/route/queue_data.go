package common

type QueueData struct {
	BrokerName string
	ReadQueueNums int
	WriteQueueNums int
	Perm int
	TopicSysFlag int
}

