package internal

import "time"

type KeyValueStore struct {
	store     map[string]Item
	expireMap map[string]ExpiryMetadata
}

type Item struct {
	value interface{}
}

type ExpiryMetadata struct {
	expireTimestamp    int64
	timeInMilliseconds bool
}

var kvStore = KeyValueStore{
	store:     make(map[string]Item),
	expireMap: make(map[string]ExpiryMetadata),
}

func (kv *KeyValueStore) Get(key string) (interface{}, bool) {
	item, exists := kv.store[key]
	if !exists {
		return "", false
	}
	if kv.isExpired(key) {
		delete(kv.store, key)
		return "", false
	}
	return item.value, true
}

func (kv *KeyValueStore) isExpired(key string) bool {
	expireInfo, exists := kv.expireMap[key]
	if !exists {
		return false
	}

	var curTime int64
	if expireInfo.timeInMilliseconds {
		curTime = time.Now().UnixMilli()
	} else {
		curTime = time.Now().Unix()
	}

	if expireInfo.expireTimestamp < curTime {
		return true
	}

	return false
}

func (kv *KeyValueStore) Set(key string, value interface{}, expiryTime int64, expiryInMillseconds bool) {

	if expiryTime != 0 {
		kv.expireMap[key] = ExpiryMetadata{
			expireTimestamp:    expiryTime,
			timeInMilliseconds: expiryInMillseconds,
		}
		if kv.isExpired(key) {
			delete(kv.expireMap, key)
			return
		}
	}

	kv.store[key] = Item{
		value: value,
	}
}

func (kv *KeyValueStore) Size() int {
	return len(kv.store)
}

func (kv *KeyValueStore) ExpiryTableSize() int {
	return len(kv.expireMap)
}

type KeyValueWithExpiry struct {
	Key                string
	Value              interface{}
	ExpiryTime         int64
	TimeInMilliseconds bool
}

func (kv *KeyValueStore) Items() []KeyValueWithExpiry {
	var items []KeyValueWithExpiry
	for key, item := range kv.store {
		if kv.isExpired(key) {
			delete(kv.store, key)
			continue
		}

		var expiryTime int64
		var timeInMilliseconds bool

		if expiryInfo, exists := kv.expireMap[key]; exists {
			expiryTime = expiryInfo.expireTimestamp
			timeInMilliseconds = expiryInfo.timeInMilliseconds
		}

		items = append(items, KeyValueWithExpiry{
			Key:                key,
			Value:              item.value,
			ExpiryTime:         int64(expiryTime),
			TimeInMilliseconds: timeInMilliseconds,
		})
	}
	return items
}
