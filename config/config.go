package config

import "net"

type RedisConfig struct {
	Port      string
	IpAddress string
}

type ReplicationInfo struct {
	Role             string
	MasterHost       string
	MasterPort       string
	MasterReplId     string
	MasterReplOffset int
	MasterConn       net.Conn
}

var InstanceConfig RedisConfig
var InstReplicationInfo ReplicationInfo
