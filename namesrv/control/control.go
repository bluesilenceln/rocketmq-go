package control

import (
	"os"
	. "rocketmq-go/logging"
	. "rocketmq-go/namesrv/config"
	. "rocketmq-go/namesrv/kvconfig"
	. "rocketmq-go/namesrv/routeinfo"
	. "rocketmq-go/namesrv/scheduler"
	. "rocketmq-go/remote"
	"time"
)

const (
	brokerActiveCheck = 0
	kvConfigPrint = 1
)

type Control struct {
	RemoteSrv Service
	RouteInfo *RouteInfo
	KVConfig *KVConfig
	NameSrvConf *Config

	scheduler *Scheduler
	stopChan chan os.Signal
}

func NewControl(confPath string, stopChan chan os.Signal) *Control {
	var control Control
	control.RouteInfo = NewRouteInfo()
	control.KVConfig = NewKVConfig()
	control.NameSrvConf = NewConfig(confPath)
	control.scheduler = NewScheduler()
	control.stopChan = stopChan

	return &control
}

func (c *Control) Start() {
	c.RemoteSrv.Start()
	c.scheduler.Add(brokerActiveCheck, 10 * time.Second, c.RouteInfo.ScanNotActiveBroker)
	c.scheduler.Add(kvConfigPrint, 10 * time.Minute, c.KVConfig.PrintAllPeriodically)
	c.scheduler.Start()
}

func (c *Control) Stop() {
	sig := <- c.stopChan
	Log.Sugar().Debugf("Signal: %v", sig)
	c.RemoteSrv.Stop()
	c.scheduler.Stop()
}

