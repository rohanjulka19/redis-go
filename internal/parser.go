package internal

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func ParseRESP(reader *bufio.Reader) (interface{}, error) {
	prefix, err := reader.Peek(1)
	if err != nil {
		return nil, fmt.Errorf("failed to peek at RESP type: %v", err)
	}

	switch prefix[0] {
	case '+':
		return parseSimpleString(reader)
	case '-':
		return parseSimpleError(reader)
	case ':':
		return parseInteger(reader)
	case '$':
		return parseBulkStrings(reader)
	case '*':
		return ParseArray(reader)
	default:
		return nil, fmt.Errorf("unkown RESP type: %c", prefix[0])
	}
}

func parseSimpleString(reader *bufio.Reader) (string, error) {
	message, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to parse simple string: %v", err)
	}

	return strings.TrimSuffix(message, "\r\n"), nil
}

func parseSimpleError(reader *bufio.Reader) (string, error) {
	message, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to parse simple error: %v", err)
	}

	return strings.TrimSuffix(message, "\r\n"), nil
}

func parseInteger(reader *bufio.Reader) (int, error) {
	message, err := reader.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("failed to parse integer: %v", err)
	}

	numberStr := strings.TrimSuffix(message, "\r\n")

	number, err := strconv.ParseInt(numberStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer '%s':  '%v'", numberStr, err)
	}
	return int(number), nil
}

func parseBulkStrings(reader *bufio.Reader) (interface{}, error) {
	lengthStr, _ := reader.ReadString('\n')

	lengthStr = strings.TrimSuffix(lengthStr[1:], "\r\n")

	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid bulk string '%s': length is not a valid integer", lengthStr)
	}

	if length == -1 {
		return nil, nil
	}
	data := make([]byte, length+2)
	_, _ = io.ReadFull(reader, data)

	res := string(data)
	return res[:length], nil
}

func ParseArray(reader *bufio.Reader) ([]interface{}, error) {
	lengthStr, _ := reader.ReadString('\n')

	lengthStr = strings.TrimSuffix(lengthStr[1:], "\r\n")
	length, _ := strconv.ParseInt(lengthStr, 10, 64)

	if length == -1 {
		return nil, nil
	}

	parsedArr := []interface{}{}

	for i := 0; i < int(length); i++ {
		arrElem, err := ParseRESP(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to parse array element '%d' : %v", i, err)
		}

		parsedArr = append(parsedArr, arrElem)
	}

	return parsedArr, nil
}

func parseOptions(args []interface{}, validOptions map[string]bool) (map[string]string, error) {

	parsedArgs := make(map[string]string)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		argStr, _ := arg.(string)
		isFlag, exists := validOptions[argStr]
		if !exists {
			return nil, fmt.Errorf("unsupported argument: %s", arg)
		}
		if isFlag {
			parsedArgs[argStr] = ""
		} else {
			if len(args) <= i+1 {
				return nil, fmt.Errorf("argument %s requires a value", arg)
			} else {
				parsedArgs[argStr] = args[i+1].(string)
				i++
			}
		}
	}
	return parsedArgs, nil
}
