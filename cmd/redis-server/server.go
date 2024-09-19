package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"

	config "myredis/config"
	internal "myredis/internal"
)

func main() {
	fmt.Println("Starting Redis Server...")

	config.InstReplicationInfo.MasterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	config.InstReplicationInfo.MasterReplOffset = 0

	port := pflag.String("port", "6377", "--port to set the port number")
	replicaOf := pflag.String("replicaof", "", "--replicaof '<Master_Host> <Master_Port>' ")
	pflag.Parse()

	config.InstReplicationInfo.Role = "master"
	config.InstanceConfig.Port = *port

	if *replicaOf != "" {
		masterDetails := strings.Split(*replicaOf, "")
		config.InstReplicationInfo.MasterHost = masterDetails[0]
		config.InstReplicationInfo.MasterPort = masterDetails[1]
		config.InstReplicationInfo.Role = "slave"
		internal.HandshakeWithMaster()
	}

	tasksChannel := make(chan func())
	internal.SpawnWorkers(10, tasksChannel)

	if config.InstReplicationInfo.Role == "master" {
		loadRdbFile()
	}

	url := fmt.Sprintf("0.0.0.0:%s", *port)
	listener, err := net.Listen("tcp", url)

	if err != nil {
		fmt.Errorf("Failed to bind to port: %v", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Accepting connections")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		tasksChannel <- func() {
			handleConn(conn)
		}
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		parsedArr, err := internal.ParseArray(reader)
		resp := ""

		if err != nil {
			if err == io.EOF {
				break
			}
			conn.Write([]byte(fmt.Sprintf("failed to parse command: %v", err)))
			continue
		}
		command, ok := parsedArr[0].(string)
		if !ok {
			conn.Write([]byte(fmt.Sprintf("command has to be string: %v", parsedArr[0])))
			continue
		}

		args := []interface{}{}
		if len(parsedArr) > 1 {
			args = parsedArr[1:]
		}

		resp, err = internal.Handle(command, args)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("invalid command: %v", err)))
			continue
		}
		conn.Write([]byte(resp))
	}
}

func loadRdbFile() {
	rdbFilePath := filepath.Join(internal.Config["dir"], internal.Config["dbfilename"])
	internal.ParseRdbFile(rdbFilePath)
}
