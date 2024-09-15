package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	internal "myredis/internal"
)

func main() {
	fmt.Println("Starting Redis Server")

	tasksChannel := make(chan func())
	internal.SpawnWorkers(10, tasksChannel)
	loadRdbFile()

	listener, err := net.Listen("tcp", "0.0.0.0:6378")
	defer listener.Close()

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
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
