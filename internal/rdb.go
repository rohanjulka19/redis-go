package internal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"

	lzf "github.com/zhuyie/golzf"
)

func initialiseRDBFile(isTemp bool) (*os.File, error) {
	fileName := Config["dbfilename"]
	fileDir := Config["dir"]
	filePath := filepath.Join(fileDir, fileName)

	os.MkdirAll(fileDir, 0755)
	file, _ := os.Create(filePath)
	file.Write([]byte("REDIS0007"))
	return file, nil
}

func addKeyValueToRdbFile(file *os.File, key string, value interface{}, expiryTime uint64, expiryInMilliseconds bool) error {
	file.Seek(0, io.SeekEnd)
	encodedKey, err := encodeString(key)
	if err != nil {
		return fmt.Errorf("failed to encode key value - %s : %v", key, err)
	}
	var valueType uint8
	var encodedValue []byte
	switch t := value.(type) {
	case string:
		encodedValue, err = encodeString(t)
		valueType = 0
		if err != nil {
			return fmt.Errorf("failed to encode key value - %s : %v", value, err)
		}
	case int:
		encodedValue, err = encodeIntegerAsString(t)
		valueType = 0
		if err != nil {
			return fmt.Errorf("failed to encode key value - %s : %v", value, err)
		}

	}

	var buffer bytes.Buffer

	if expiryTime > 0 {
		var encodedExpiryTime []byte
		var startByte []byte

		if expiryInMilliseconds {
			encodedExpiryTime = make([]byte, 8)
			startByte = []byte{byte(0xFC)}
			binary.LittleEndian.PutUint64(encodedExpiryTime, expiryTime)
		} else {
			encodedExpiryTime = make([]byte, 4)
			startByte = []byte{byte(0xFD)}
			binary.LittleEndian.PutUint32(encodedExpiryTime, uint32(expiryTime))
		}

		buffer.Write(startByte)
		buffer.Write(encodedExpiryTime)
	}

	buffer.Write([]byte{byte(valueType)})
	buffer.Write(encodedKey)
	buffer.Write(encodedValue)

	file.Write(buffer.Bytes())
	return nil
}

func addAuxFieldToRdbFile(file *os.File, key string, value interface{}) error {
	file.Seek(0, io.SeekEnd)
	encodedKey, err := encodeString(key)
	if err != nil {
		return fmt.Errorf("failed to encode key value - %s : %v", key, err)
	}

	var encodedValue []byte
	switch t := value.(type) {
	case string:
		encodedValue, err = encodeString(t)
		if err != nil {
			return fmt.Errorf("failed to encode - %s : %v", value, err)
		}

	case int:
		encodedValue, err = encodeIntegerAsString(t)
		if err != nil {
			return fmt.Errorf("failed to encode - %s : %v", value, err)
		}
	}

	var buffer bytes.Buffer
	startByte := 0xFA
	buffer.Write([]byte{byte(startByte)})
	buffer.Write(encodedKey)
	buffer.Write(encodedValue)

	file.Write(buffer.Bytes())
	return nil
}

func addDatabaseSelector(file *os.File, db int) error {
	file.Seek(0, io.SeekEnd)

	encodedBytes, err := encodeLength(db, false, -1)
	if err != nil {
		return fmt.Errorf("failed to encode db number: %v", err)
	}

	var startByte = 0xFE
	var buffer bytes.Buffer
	buffer.Write([]byte{byte(startByte)})
	buffer.Write(encodedBytes)
	// buffer.WriteString("\n")

	file.Write(buffer.Bytes())

	return nil
}

func addResizeDBInfo(file *os.File, tableSize int, expiryTableSize int) error {
	file.Seek(0, io.SeekEnd)

	encodedTableSize, err := encodeLength(tableSize, false, -1)
	if err != nil {
		return fmt.Errorf("failed to encode hash table size: %v", err)
	}

	encodedExpiryTableSize, err1 := encodeLength(expiryTableSize, false, -1)
	if err1 != nil {
		return fmt.Errorf("failed to encode expiry hash table size: %v", err1)
	}

	var startByte = 0xFB
	var buffer bytes.Buffer
	buffer.Write([]byte{byte(startByte)})
	buffer.Write(encodedTableSize)
	buffer.Write(encodedExpiryTableSize)

	file.Write(buffer.Bytes())

	return nil
}

func addCheckSumToRdbFile(file *os.File) error {
	file.Seek(0, io.SeekEnd)

	var encodedChecksum = make([]byte, 8)
	_, _ = file.Seek(0, io.SeekStart)
	data, _ := io.ReadAll(file)
	table := crc64.MakeTable(crc64.ECMA)
	checksum := crc64.Checksum(data, table)
	binary.BigEndian.AppendUint64(encodedChecksum, checksum)
	_, _ = file.Seek(0, io.SeekEnd)

	var buffer bytes.Buffer
	startByte := 0xFF
	buffer.Write([]byte{byte(startByte)})
	buffer.Write(encodedChecksum)

	file.Write(buffer.Bytes())

	return nil
}

func encodeList(list []interface{}) ([]byte, error) {
	var byteEncodedList []byte
	encodedLength, err := encodeLength(len(list), false, -1)
	byteEncodedList = append(byteEncodedList, encodedLength...)
	if err != nil {
		return nil, fmt.Errorf("failed to encode list legth: %v", err)
	}

	for i, item := range list {
		switch t := item.(type) {
		case string:
			encodedBytes, err := encodeString(t)
			if err != nil {
				return nil, fmt.Errorf("failed to encode item %d of list: %v", i, err)
			}
			byteEncodedList = append(byteEncodedList, encodedBytes...)
		case int:
			encodedBytes, err := encodeIntegerAsString(t)
			if err != nil {
				return nil, fmt.Errorf("failed to encode item %d of list: %v", i, err)
			}
			byteEncodedList = append(byteEncodedList, encodedBytes...)

		case []interface{}:
			encodedBytes, err := encodeArray(t)
			if err != nil {
				return nil, fmt.Errorf("failed to encode item %d of list: %v", i, err)
			}
			byteEncodedList = append(byteEncodedList, encodedBytes...)
		}
	}

	return byteEncodedList, nil
}

func getIntType(num int) (int, error) {

	if num <= math.MaxInt8 {
		return 0, nil
	}
	if num <= math.MaxInt16 {
		return 1, nil
	}
	if num <= math.MaxInt32 {
		return 2, nil
	}

	return 0, fmt.Errorf("error: number too large to be encoded as integer")
}

func encodeIntegerAsString(num int) ([]byte, error) {

	numType, err := getIntType(num)
	if numType < 0 || numType > 2 {
		return nil, fmt.Errorf("error: number type should be between (0-2) got: %v", err)
	}

	encodedLength, err := encodeLength(-1, true, numType)
	if err != nil {
		return nil, fmt.Errorf("failed to encode number type: %v", err)
	}

	var encodedNumber []byte
	switch numType {
	case 0:
		encodedNumber = []byte{byte(num)}
	case 1:
		encodedNumber = make([]byte, 2)
		binary.LittleEndian.PutUint16(encodedNumber, uint16(num))
	case 2:
		encodedNumber = make([]byte, 4)
		binary.LittleEndian.PutUint32(encodedNumber, uint32(num))
	}
	encodedByte := append(encodedLength, encodedNumber...)
	return encodedByte, nil
}

func encodeString(input string) ([]byte, error) {
	encodedLength, err := encodeLength(len(input), false, -1)
	if err != nil {
		return nil, fmt.Errorf("filed to encode string length: %v", err)
	}
	encodedString := []byte(input)
	encodedBytes := append(encodedLength, encodedString...)
	return encodedBytes, nil
}

func encodeLength(length int, isSpecialType bool, dataType int) ([]byte, error) {
	if (length < 0 || length > 2147483648) && !isSpecialType {
		return nil, fmt.Errorf("length must be in the range [0, 2^31 - 1]: %d", length)
	}

	if isSpecialType {
		var encodedByte byte
		encodedByte = byte(dataType)
		encodedByte |= (1 << 6)
		encodedByte |= (1 << 7)
		return []byte{encodedByte}, nil
	}

	if length <= 63 {
		return []byte{byte(length)}, nil
	}

	if length <= 16383 {
		var encodedBytes = make([]byte, 2)
		binary.BigEndian.PutUint16(encodedBytes, uint16(length))
		encodedBytes[0] |= (1 << 6)
		return encodedBytes, nil
	}

	var encodedBytes = make([]byte, 5)
	binary.BigEndian.PutUint32(encodedBytes[1:], uint32(length))
	encodedBytes[0] |= (1 << 7)
	return encodedBytes, nil
}

func ParseRdbFile(rdbFileName string) error {
	file, _ := os.Open(rdbFileName)
	defer file.Close()

	reader := bufio.NewReaderSize(file, 4096)
	headerBytes := make([]byte, 9)
	reader.Read(headerBytes)
	headerString := string(headerBytes)
	magicString := headerString[0:5]

	if magicString != "REDIS" {
		return fmt.Errorf("expected magin string 'REDIS' at the start of file")
	}

	for {
		var expiryTimeStamp uint64
		var expiryInMilliseconds bool
		opCode, _ := reader.Peek(1)

		if opCode[0] == 0xFF {
			reader.ReadByte()
			break
		}

		if opCode[0] == 0xFA {
			reader.ReadByte()
			parseStringEncoding(reader) // key
			parseStringEncoding(reader) // value
			continue
		}

		if opCode[0] == 0xFB {
			reader.ReadByte()
			parseLengthEncoding(reader) // HashTableSize
			parseLengthEncoding(reader) // ExpiryTableSize
			continue
		}

		if opCode[0] == 0xFE {
			reader.ReadByte()
			parseLengthEncoding(reader) // DB ID
			continue
		}

		if opCode[0] == 0xFD {
			// Set Expiry in seconds
			reader.ReadByte()
			var expiryBytes = make([]byte, 4)
			reader.Read(expiryBytes)
			expiryTimeStamp = uint64(binary.LittleEndian.Uint32(expiryBytes))
			expiryInMilliseconds = false
		}

		if opCode[0] == 0xFC {
			// Set Expiry in ms
			reader.ReadByte()
			var expiryBytes = make([]byte, 8)
			reader.Read(expiryBytes)
			expiryTimeStamp = binary.LittleEndian.Uint64(expiryBytes)
			expiryInMilliseconds = true
		}

		valueType, _ := reader.ReadByte()
		parseValue := getValueParser(valueType)
		key := parseStringEncoding(reader).(string)
		value := parseValue(reader)
		kvStore.Set(key, value, int64(expiryTimeStamp), expiryInMilliseconds)

	}
	return nil
}

func parseLengthEncoding(reader *bufio.Reader) (int, int) {
	firstByte, _ := reader.ReadByte()
	eigthBit := (firstByte & (1 << 7) >> 7)
	seventhBit := (firstByte & (1 << 6) >> 6)
	if eigthBit == 0 && seventhBit == 0 {
		return int(firstByte), -1
	}

	if eigthBit == 0 && seventhBit == 1 {
		firstByte = firstByte & 0b00111111
		secondByte, _ := reader.ReadByte()
		var length uint16
		length = binary.LittleEndian.Uint16([]byte{firstByte, secondByte})
		return int(length), -1
	}

	if eigthBit == 1 && seventhBit == 0 {
		lengthBytes := make([]byte, 4)
		reader.Read(lengthBytes)
		var length uint32
		length = binary.LittleEndian.Uint32(lengthBytes)
		return int(length), -1
	}

	if eigthBit == 1 && seventhBit == 1 {
		firstByte = firstByte & 0b00111111
		switch int(firstByte) {
		case 0:
			return 1, -2
		case 1:
			return 2, -2
		case 2:
			return 4, -2
		case 3:
			clen, _ := parseLengthEncoding(reader)
			uclen, _ := parseLengthEncoding(reader)
			return clen, uclen
		}
	}
	return -1, -1
}

type parseFunType func(*bufio.Reader) interface{}

func getValueParser(valueType byte) parseFunType {
	return []parseFunType{
		parseStringEncoding,
	}[int(valueType)]
}

func parseStringEncoding(reader *bufio.Reader) interface{} {
	length, uclen := parseLengthEncoding(reader)
	if uclen == -1 {
		valueBytes := make([]byte, length)
		reader.Read(valueBytes)
		return string(valueBytes)
	}

	if uclen == -2 {
		valueBytes := make([]byte, length)
		reader.Read(valueBytes)
		switch length {
		case 1:
			return int(valueBytes[0])
		case 2:
			return int(binary.LittleEndian.Uint16(valueBytes))
		case 4:
			return int(binary.LittleEndian.Uint32(valueBytes))
		}
	}

	valueBytes := make([]byte, length)
	unCompressedValue := make([]byte, uclen)
	lzf.Decompress(valueBytes, unCompressedValue)
	return unCompressedValue
}
