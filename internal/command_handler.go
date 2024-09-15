package internal

import (
	"fmt"
	"strconv"
	"time"
)

func Handle(command string, args []interface{}) (string, error) {
	switch command {
	case "PING":
		return handlePing()
	case "ECHO":
		return handleEcho(args)
	case "SET":
		return handleSet(args)
	case "GET":
		return handleGet(args)
	case "CONFIG":
		return handleConfig(args)
	case "SAVE":
		return handleSave()
	default:
		return "", fmt.Errorf("unknown command: %s", command)
	}
}

func handlePing() (string, error) {
	return encodeSimpleString("PONG"), nil
}

func handleEcho(args []interface{}) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("failed to execute ECHO command missing message")
	}

	message, _ := args[0].(string)

	return encodeBulkString(&message), nil
}

func handleSet(args []interface{}) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("failed to execute SET command, it requires a key and a value")
	}

	key, _ := args[0].(string)
	value, _ := args[1].(string)
	var parsedArgs map[string]string
	if len(args) > 2 {
		parsedArgs, _ = parseOptions(args[2:], map[string]bool{"PX": false})
	}

	var px float64
	var expireTime int64
	var expiryInMillseconds bool
	if pxStr, exists := parsedArgs["PX"]; exists {
		px, _ = strconv.ParseFloat(pxStr, 64)
		expireTime = time.Now().Add(time.Duration(px) * time.Millisecond).UnixMilli()
		expiryInMillseconds = true
	}

	kvStore.Set(key, value, expireTime, expiryInMillseconds)
	return encodeSimpleString("OK"), nil
}

func handleGet(args []interface{}) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("failed to execute GET command, it requires a key to fetch")
	}
	key, _ := args[0].(string)

	if value, exists := kvStore.Get(key); exists {
		switch t := value.(type) {
		case string:
			return encodeBulkString(&t), nil
		case int:
			return encodeInteger(t), nil
		default:
			return "", fmt.Errorf("unsupported type %T", t)
		}
	} else {
		return encodeBulkString(nil), nil
	}

}

func handleConfig(args []interface{}) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("failed to execute CONFIG command, it requirest atleast one operation")
	}
	operation := args[0]
	switch operation {
	case "GET":
		if len(args) < 2 {
			return "", fmt.Errorf("failed to execute CONFIG GET command, it requires atleast one key")
		}
		return handleConfigGet(args[1:])
	default:
		return "", fmt.Errorf("CONFIG operation %s is unsupported", operation)
	}
}

func handleConfigGet(args []interface{}) (string, error) {
	var configValues []interface{}
	for _, key := range args {
		keyStr, _ := key.(string)
		value, exists := Config[keyStr]
		if !exists {
			return "", fmt.Errorf("CONFIG GET key %s does not exists", key)
		}
		configValues = append(configValues, key)
		configValues = append(configValues, value)
	}
	return encodeArray(configValues)
}

func handleSave() (string, error) {
	rdbFile, _ := initialiseRDBFile(true)
	addAuxFieldToRdbFile(rdbFile, "redis-bits", int(64))
	addAuxFieldToRdbFile(rdbFile, "ctime", int(time.Now().Unix()))
	addDatabaseSelector(rdbFile, 1)
	addResizeDBInfo(rdbFile, kvStore.Size(), kvStore.ExpiryTableSize())
	for _, item := range kvStore.Items() {
		err := addKeyValueToRdbFile(rdbFile, item.Key, item.Value, uint64(item.ExpiryTime), item.TimeInMilliseconds)
		if err != nil {
			return "", fmt.Errorf("failed to add key %s value %s in rdb file: %v", item.Key, item.Value, err)
		}
	}
	addCheckSumToRdbFile(rdbFile)
	return encodeSimpleString("OK"), nil
}
