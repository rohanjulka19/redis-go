package main

import (
	"bufio"
	"fmt"
	"io"
	"myredis/internal"
	"net"
	"os"
	"testing"
	"time"
)

var conn net.Conn

func TestMain(t *testing.T) {
	copyRdbDumpToSourceDir()

	go main()
	conn, _ = net.Dial("tcp", "localhost:6377")
	t.Run("Test RDB File Load", testRDBLoad)
	t.Run("Echo Command Test", testEchoCommand)
	t.Run("SET Command Test", testSetCommand)
	t.Run("GET Command Test", testGetCommand)
	t.Run("GET Non Existent Value", testGetValueDoesNotExist)
	t.Run("SET and GET value with expiry", testSetAndGetValueWithExpiry)
	t.Run("Save RDB File", testSaveCommand)
	t.Run("Test CONFIG Get Command", testConfigGet)
	t.Run("Test KEYS Command", testKeysCommand)
	t.Run("Test INFO command", testInfoCommand)
}

func testEchoCommand(t *testing.T) {
	runCommandTest(t, "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", "$3\r\nhey\r\n", 9, conn)
}

func testSetCommand(t *testing.T) {
	runCommandTest(t, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", "+OK\r\n", 5, conn)
}

func testGetCommand(t *testing.T) {
	runCommandTest(t, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$3\r\nbar\r\n", 9, conn)
}

func testGetValueDoesNotExist(t *testing.T) {
	runCommandTest(t, "*2\r\n$3\r\nGET\r\n$3\r\nfog\r\n", "$-1\r\n", 5, conn)
}

func testSetAndGetValueWithExpiry(t *testing.T) {
	runCommandTest(t, "*5\r\n$3\r\nSET\r\n$3\r\ncow\r\n$3\r\nsay\r\n$2\r\nPX\r\n$4\r\n2000\r\n", "+OK\r\n", 5, conn)
	runCommandTest(t, "*2\r\n$3\r\nGET\r\n$3\r\ncow\r\n", "$3\r\nsay\r\n", 9, conn)
	time.Sleep(2 * time.Second)
	runCommandTest(t, "*2\r\n$3\r\nGET\r\n$3\r\ncow\r\n", "$-1\r\n", 5, conn)
}

func testSaveCommand(t *testing.T) {
	runCommandTest(t, "*5\r\n$3\r\nSET\r\n$3\r\ncow\r\n$3\r\nsay\r\n$2\r\nPX\r\n$6\r\n120000\r\n", "+OK\r\n", 5, conn)
	runCommandTest(t, "*1\r\n$4\r\nSAVE\r\n", "+OK\r\n", 5, conn)
}

func testRDBLoad(t *testing.T) {
	runCommandTest(t, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$3\r\nbar\r\n", 9, conn)
	runCommandTest(t, "*2\r\n$3\r\nGET\r\n$3\r\ncow\r\n", "$-1\r\n", 5, conn)

}

func testConfigGet(t *testing.T) {
	tempDir := internal.Config["dir"]
	tempDbFileName := internal.Config["dbfilename"]
	internal.Config["dir"] = "/tmp/dir"
	internal.Config["dbfilename"] = "dump.rdb"

	runCommandTest(t, "*4\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n$10\r\ndbfilename\r\n",
		"*4\r\n$3\r\ndir\r\n$8\r\n/tmp/dir\r\n$10\r\ndbfilename\r\n$8\r\ndump.rdb\r\n", 58, conn)

	internal.Config["dir"] = tempDir
	internal.Config["dbfilename"] = tempDbFileName
}

func testKeysCommand(t *testing.T) {
	runCommandTest(t, "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n", "*2\r\n$3\r\nfoo\r\n$3\r\ncow\r\n", 22, conn)
}

func testInfoCommand(t *testing.T) {
	runCommandTest(t, "*2\r\n$4\r\nINFO\r\n$10\r\nreplication\r\n", "$87\r\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n", 94, conn)
}

func runCommandTest(t *testing.T, command string, expectedResp string, respByteCount int, conn net.Conn) {
	_, err := conn.Write([]byte(command))
	if err != nil {
		t.Fatalf("Failed to send command: %v", err)
	}

	resp := make([]byte, respByteCount)
	reader := bufio.NewReader(conn)
	_, err = reader.Read(resp)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	if string(resp) != expectedResp {
		t.Errorf("Error: Expected %s, Got %s", expectedResp, resp)
	}

}

func copyRdbDumpToSourceDir() {
	internal.Config["dir"] = "../dump"
	internal.Config["dbfilename"] = "dump.rdb"
	err := os.MkdirAll("../dump", 0755)
	if err != nil {
		fmt.Errorf("failed to create directory")
	}

	src, err := os.Open("../test/dump.rdb")
	if err != nil {
		fmt.Errorf("failed to open directory")
	}
	defer src.Close()

	dest, err := os.Create("../dump/dump.rdb")
	if err != nil {
		fmt.Errorf("failed to create directory in destination")
	}
	defer dest.Close()

	_, err = io.Copy(dest, src)
	if err != nil {
		fmt.Errorf("failed to copy file")
	}

}
