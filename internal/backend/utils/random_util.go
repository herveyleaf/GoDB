package utils

import "crypto/rand"

func RandomBytes(length int) []byte {
	buf := make([]byte, length)
	rand.Read(buf)
	return buf
}
