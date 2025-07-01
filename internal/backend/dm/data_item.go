package dm

import (
	"sync"

	"github.com/herveyleaf/GoDB/internal/backend/utils"
)

const (
	OF_VALID         int = 0
	OF_SIZE_DATAITEM int = 1
	OF_DATA_DATAITEM int = 3
)

type DataItem interface {
	Data() []byte
	Before()
	UnBefore()
	After(xid int64)
	Release()
	Lock()
	Unlock()
	RLock()
	RUnLock()
	Page() Page
	GetUid() int64
	GetOldRaw() []byte
	GetRaw() []byte
}

type DataItemImpl struct {
	raw    []byte
	oldRaw []byte
	lock   sync.RWMutex
	dm     DataManagerImpl
	uid    int64
	pg     Page
}

func NewDataItemImpl(raw []byte, oldRaw []byte, pg Page, uid int64, dm DataManagerImpl) *DataItemImpl {
	return &DataItemImpl{
		raw:    raw,
		oldRaw: oldRaw,
		lock:   sync.RWMutex{},
		dm:     dm,
		uid:    uid,
		pg:     pg,
	}
}

func WrapDataItemRaw(raw []byte) []byte {
	valid := make([]byte, 1)
	size := utils.Short2Byte(int16(len(raw)))
	return append(append(valid, size...), raw...)
}

func ParseDataItem(pg Page, offset int16, dm DataManagerImpl) DataItem {
	raw := pg.GetData()
	size := utils.ParseShort(raw[offset+int16(OF_SIZE_DATAITEM) : offset+int16(OF_DATA_DATAITEM)])
	length := int16(size + int16(OF_DATA_DATAITEM))
	uid := utils.AddressToUid(pg.GetPageNumber(), offset)
	return NewDataItemImpl(raw[offset:offset+length], make([]byte, length), pg, uid, dm)
}

func SetDataItemRawInvalid(raw []byte) {
	raw[OF_VALID] = byte(1)
}

func (di *DataItemImpl) IsValid() bool {
	return di.raw[OF_VALID] == byte(0)
}

func (di *DataItemImpl) Data() []byte {
	return di.raw[OF_DATA_DATAITEM:]
}

func (di *DataItemImpl) Before() {
	di.lock.Lock()
	di.pg.SetDirty(true)
	copy(di.oldRaw, di.raw[:len(di.oldRaw)])
}

func (di *DataItemImpl) UnBefore() {
	copy(di.raw, di.oldRaw[:len(di.oldRaw)])
	di.lock.Unlock()
}

func (di *DataItemImpl) After(xid int64) {
	di.dm.LogDataItem(xid, di)
	di.lock.Unlock()
}

func (di *DataItemImpl) Release() {
	di.dm.ReleaseDataItem(di)
}

func (di *DataItemImpl) Lock() {
	di.lock.Lock()
}

func (di *DataItemImpl) Unlock() {
	di.lock.Unlock()
}

func (di *DataItemImpl) RLock() {
	di.lock.RLock()
}

func (di *DataItemImpl) RUnLock() {
	di.lock.RUnlock()
}

func (di *DataItemImpl) Page() Page {
	return di.pg
}

func (di *DataItemImpl) GetUid() int64 {
	return di.uid
}

func (di *DataItemImpl) GetOldRaw() []byte {
	return di.oldRaw
}

func (di *DataItemImpl) GetRaw() []byte {
	return di.raw
}
