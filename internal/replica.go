package internal

import (
	"bufio"
	"fmt"
	"io"
	"myredis/config"
	"myredis/internal"
	"net"
	"os"
	"path/filepath"
)

type ReplicaConfig struct {
	Port      string
	IpAddress string
	Capa      []string
}

var Replicas []ReplicaConfig

func HandshakeWithMaster() error {
	reader, err := sendToMaster(encodeSimpleError("PING"))
	if err != nil {
		return err
	}
	resp, _ := parseSimpleString(reader)
	if resp != "PONG" {
		return fmt.Errorf("invalid response from master on PING when performing handshake: %s", resp)
	}
	ip := config.InstanceConfig.IpAddress
	port := config.InstanceConfig.Port
	announceReplicaConfig := []interface{}{"REPLCONF", "ip-address", ip, "listening-port", port, "capa", "psync2"}
	encodedCommand, err := encodeArray(announceReplicaConfig)
	reader, err = sendToMaster(encodedCommand)
	if err != nil {
		return err
	}
	resp, _ = parseSimpleString(reader)
	if resp != "OK" {
		return fmt.Errorf("expected OK response from master when Sending Replica during handshake: %s", resp)
	}

	syncCommand := []interface{}{"PSYNC", "?", "-1"}
	encodedCommand, err = encodeArray(syncCommand)
	if err != nil {
		return err
	}
	reader, err = sendToMaster(encodedCommand)
	if err != nil {
		return err
	}
	resp, _ = parseSimpleString(reader)

	return nil
}

func sendToMaster(command string) (*bufio.Reader, error) {
	conn := getMasterConnection()
	_, err := conn.Write([]byte(command))
	if err != nil {
		return nil, fmt.Errorf("failed to send command %s error: %v", command, err)
	}
	reader := bufio.NewReader(conn)

	return reader, nil
}

func getMasterConnection() net.Conn {
	replicationInfo := config.InstReplicationInfo
	if replicationInfo.MasterConn == nil {
		masterUrl := fmt.Sprintf("%s:%s", replicationInfo.MasterHost, replicationInfo.MasterPort)
		replicationInfo.MasterConn, _ = net.Dial("tcp", masterUrl)
	}
	return replicationInfo.MasterConn
}

func sendRdbToReplica() error {
	rdbFilePath := filepath.Join(internal.Config["dir"], internal.Config["dbfilename"])
	file, err := os.Open(rdbFilePath)
	if err != nil {
		return fmt.Errorf("failed to open rdb file to send to replica error: %v", err)
	}

	rdbContent, _ := io.ReadAll(file)
	encodedRdbContent := encodeBulkString(&rdbFilePath)

	return nil
}
