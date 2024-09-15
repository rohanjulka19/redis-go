package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Failed to connect to Redis server:", err)
		os.Exit(1)
	}
	defer conn.Close()
	
	conn.Write([]byte("PING"))
	response := make([]byte, 4096)
	n, err := conn.Read(response)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	fmt.Println(string(response[:n]))
}