package config

import (
	"github.com/BurntSushi/toml"
	"path/filepath"
	"sync"
)

var (
	cfg * Config
	once sync.Once
)

type Config struct {
	RocketmqHome string `toml:"rocketMQHome"`
	KVConfigPath string `toml:"kvConfigPath"`
	ConfigStorePath string `toml:"configStorePath"`
	ClusterTest bool `toml:"clusterTest"`
	OrderMessageEnable bool `toml:"orderMessageEnable"`
	ListenAddr string `toml:"listenAddr"`
}

func NewConfig(confPath string) *Config {
	once.Do(func() {
		filePath, err := filepath.Abs(confPath)
		if err != nil {
			panic(err)
		}

		if _ , err := toml.DecodeFile(filePath, &cfg); err != nil {
			panic(err)
		}
	})
	return cfg
}