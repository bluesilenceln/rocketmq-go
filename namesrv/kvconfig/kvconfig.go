package kvconfig

import (
	"encoding/json"
	"go.uber.org/zap"
	common "rocketmq-go/common/proto/route"
	. "rocketmq-go/logging"
	"sync"
)

type KVConfig struct {
	rw sync.RWMutex
	configTable map[string]map[string]string
}

func NewKVConfig() *KVConfig {
	var rw sync.RWMutex
	m := make(map[string]map[string]string)
	return &KVConfig{
		rw,
		m,
	}
}

func (k *KVConfig) PrintAllPeriodically() {

}

func (k *KVConfig) persist() {

}

func (k *KVConfig) PutKVConfig(namespace string, key string, value string) {
	k.rw.Lock()
	defer k.rw.Unlock()

	kvTable, ok := k.configTable[namespace]
	if !ok {
		kvTable = make(map[string]string)
		k.configTable[namespace] = kvTable
		Log.Info("PutKVConfig create new namespace", zap.String("namespace", namespace))
	}

	_, ok = kvTable[key]
	kvTable[key] = value
	if ok {
		Log.Info("PutKVConfig update config item",
			zap.String("namespace", namespace),
			zap.String("key", key),
			zap.String("value", value))
	} else {
		Log.Info("PutKVConfig create new config item",
			zap.String("namespace", namespace),
			zap.String("key", key),
			zap.String("value", value))
	}

	k.persist()
}

func (k *KVConfig) GetKVConfig(namespace string, key string) string {
	k.rw.RLock()
	defer k.rw.RUnlock()

	kvTable, ok := k.configTable[namespace]
	if ok {
		return kvTable[key]
	}

	return ""
}

func (k *KVConfig) DeleteKVConfig(namespace string, key string) {
	k.rw.Lock()
	defer k.rw.Unlock()

	kvTable, ok := k.configTable[namespace]
	if ok {
		value := kvTable[key]
		delete(kvTable, key)
		Log.Info("DeleteKVConfig delete a config item",
			zap.String("namespace", namespace),
			zap.String("key", key),
			zap.String("value", value))
	}

	k.persist()
}

func (k *KVConfig) GetKVListByNamespace(namespace string) []byte {
	k.rw.RLock()
	defer k.rw.RUnlock()

	kvTable, ok := k.configTable[namespace]
	if ok {
		table := common.KVTable{Table: kvTable}
		data, _ := json.Marshal(table)
		return data
	}
	return nil
}
