package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"rocketmq-go/logging"
	"rocketmq-go/namesrv/control"
	"rocketmq-go/namesrv/processor"
	"rocketmq-go/remote"
	"syscall"
)

var (
	confPath string
	logPath string
	rocketHome string
)

func init() {
	rocketHome = os.Getenv("ROCKETMQ_HOME")
	if rocketHome == "" {
		log.Fatal("please set env $ROCKETMQ_HOME")
	}
	confPath = rocketHome + "/conf"
	logPath = rocketHome + "/log"
	flag.StringVar(&confPath, "c", confPath, "set configuration file")
	flag.StringVar(&logPath, "log", logPath, "set log file")
	flag.Parse()
}

func main() {
	initConfAndLog()
	logging.Init(logPath, "debug")

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGHUP)

	ctl := control.NewControl(confPath, stopChan)

	defaultProcessor := processor.NewDefaultProcessor(ctl)

	remoteSrv := remote.NewServer(ctl.NameSrvConf.ListenAddr)
	remoteSrv.Processor = defaultProcessor.Process
	ctl.RemoteSrv = remoteSrv

	ctl.Start()
	ctl.Stop()
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	return false, err
}

func isDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func getCurPath() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func initConfAndLog() {
	if !isDir(confPath) {
		log.Fatalf("config path is not dir: %s", confPath)
	}

	if !isDir(logPath) {
		log.Fatalf("log path is not dir: %s", logPath)
	}

	confPath, _ = filepath.Abs(confPath)
	confPath = confPath + string(filepath.Separator) + "namesrv.toml"
	if ok, err := exists(confPath); !ok {
		log.Fatal(err)
	}

	logPath, _ = filepath.Abs(logPath)
	logPath = logPath + string(filepath.Separator) + "namesrv.log"
}