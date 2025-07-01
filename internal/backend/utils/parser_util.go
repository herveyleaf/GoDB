package utils

import "encoding/binary"

func Short2Byte(value int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(value))
	return buf
}

func ParseShort(buf []byte) int16 {
	return int16(binary.BigEndian.Uint16(buf[:2]))
}

func Int2Byte(value int) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return buf
}

func ParseInt(buf []byte) int {
	return int(binary.BigEndian.Uint32(buf[:4]))
}

func Long2Byte(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

func ParseLong(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf[:8]))
}

func ParseString(raw []byte) *ParseStringRes {
	length := ParseInt(raw[:4])
	str := string(raw[4 : 4+length])
	return NewParseStringRes(str, length+4)
}

func String2Byte(str string) []byte {
	length := Int2Byte(len(str))
	data := []byte(str)
	return append(length, data...)
}

func Str2Uid(key string) int64 {
	seed := int64(13331)
	res := int64(0)
	for _, c := range []byte(key) {
		res = res*seed + int64(c)
	}
	return res
}
