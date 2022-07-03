package go_caskdb

import (
	"encoding/binary"
)

const headerLength = 12

func encodeHeader(timestamp uint32, keySize uint32, valueSize uint32) []byte {
	// | timestamp 4B | keySize 4B | valueSize 4B | -> total allocate 12 Byte
	b := make([]byte, 12)
	binary.LittleEndian.PutUint32(b[0:], timestamp)
	binary.LittleEndian.PutUint32(b[4:], keySize)
	binary.LittleEndian.PutUint32(b[8:], valueSize)

	return b
}

func decodeHeader(data []byte) (uint32, uint32, uint32) {
	timestamp := binary.LittleEndian.Uint32(data[0:])
	keySize := binary.LittleEndian.Uint32(data[4:])
	valueSize := binary.LittleEndian.Uint32(data[8:])

	return timestamp, keySize, valueSize
}

func encodeKV(timestamp uint32, key string, value string) (int64, []byte) {
	header := encodeHeader(timestamp, uint32(len(key)), uint32(len(value)))
	keyB := []byte(key)
	valB := []byte(value)

	length := len(header) + len(keyB) + len(valB)
	data := make([]byte, length)

	copy(data[:len(header)], header)
	copy(data[len(header):], keyB)
	copy(data[len(header)+len(keyB):], valB)

	// length will represent new offset
	return int64(length), data
}

func decodeKV(data []byte) (uint32, string, string) {
	header := data[0:headerLength]
	ts, ks, _ := decodeHeader(header)

	key := data[headerLength : headerLength+ks]
	val := data[headerLength+ks:]

	return ts, string(key), string(val)
}
