package routeinfo

type BrokerLiveInfo struct {
	lastUpdateTime int64
	//dataVersion DataVersion
	haServerAddr string
}

func NewBrokerLiveInfo(lastUpdateTime int64, haServerAddr string) *BrokerLiveInfo {
	return &BrokerLiveInfo{
		lastUpdateTime: lastUpdateTime,
		haServerAddr: haServerAddr,
	}
}

func (b *BrokerLiveInfo) GetLastUpdateTime() int64 {
	return b.lastUpdateTime
}

func (b *BrokerLiveInfo) SetLastUpdateTime(lastUpdateTime int64) {
	b.lastUpdateTime = lastUpdateTime
}

func (b *BrokerLiveInfo) GetHaServerAddr() string {
	return b.haServerAddr
}

func (b *BrokerLiveInfo) SetHaServerAddr(haServerAddr string) {
	b.haServerAddr = haServerAddr
}