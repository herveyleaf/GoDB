package utils

func AddressToUid(pgno int, offset int16) int64 {
	u0 := int64(pgno)
	u1 := int64(offset)
	return (u0 << 32) | u1
}
