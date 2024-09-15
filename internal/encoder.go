package internal

import (
	"fmt"
	"strconv"
)

func encodeSimpleString(input string) string {
	return "+" + input + "\r\n"
}

func encodeSimpleError(input string) string {
	return "-" + input + "\r\n"
}

func encodeInteger(input int) string {
	return ":" + strconv.FormatInt(int64(input), 10) + "\r\n"
}

func encodeBulkString(input *string) string {
	if input == nil {
		return "$-1\r\n"
	}
	return "$" + strconv.FormatInt(int64(len(*input)), 10) + "\r\n" + *input + "\r\n"
}

func encodeArray(input []interface{}) (string, error) {
	encodedArr := "*" + strconv.FormatInt(int64(len(input)), 10) + "\r\n"
	for i := 0; i < len(input); i++ {
		switch t := input[i].(type) {
		case string:
			encodedArr += encodeBulkString(&t)
		case int:
			encodedArr += encodeInteger(t)
		case []interface{}:
			res, err := encodeArray(t)
			if err != nil {
				return "", fmt.Errorf("failed to parse inner array: %v", err)
			}
			encodedArr += res
		default:
			return "", fmt.Errorf("failed to parse array found unknown type: %T", t)
		}
	}
	return encodedArr, nil
}
