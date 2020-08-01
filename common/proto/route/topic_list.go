package common

type TopicList struct {
	BrokerAddr string
	TopicList map[string]bool
}