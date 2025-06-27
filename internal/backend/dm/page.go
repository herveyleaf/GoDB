package dm

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"sync"
)

const (
	OF_VC          = 100
	LEN_VC         = 8
	OF_FREE        = 0
	OF_DATA        = 2
	MAX_FREE_SPACE = PAGE_SIZE - OF_DATA
)

type Page interface {
	Lock()
	Unlock()
	Release()
	SetDirty(dirty bool)
	IsDirty() bool
	GetPageNumber() int
	GetData() []byte
}

type PageImpl struct {
	pageNumber int
	data       []byte
	dirty      bool
	lock       sync.Mutex
	pc         PageCache
}

func NewPageImpl(pageNumber int, data []byte, pc PageCache) *PageImpl {
	return &PageImpl{
		pageNumber: pageNumber,
		data:       data,
		pc:         pc,
		lock:       sync.Mutex{},
	}
}

func (pgi *PageImpl) Lock() {
	pgi.lock.Lock()
}

func (pgi *PageImpl) Unlock() {
	pgi.lock.Unlock()
}

func (pgi *PageImpl) Release() {
	pgi.pc.Release(pgi)
}

func (pgi *PageImpl) SetDirty(dirty bool) {
	pgi.dirty = dirty
}

func (pgi *PageImpl) IsDirty() bool {
	return pgi.dirty
}

func (pgi *PageImpl) GetPageNumber() int {
	return pgi.pageNumber
}

func (pgi *PageImpl) GetData() []byte {
	return pgi.data
}

// 特殊管理第一页

func InitRawO() []byte {
	raw := make([]byte, PAGE_SIZE)
	setVcOpenByte(raw)
	return raw
}

func SetVcOpenPage(pg Page) {
	pg.SetDirty(true)
	setVcOpenByte(pg.GetData())
}

func setVcOpenByte(raw []byte) {
	rand.Read(raw[OF_VC : OF_VC+LEN_VC])
}

func SetVcClosePage(pg Page) {
	pg.SetDirty(true)
	setVcCloseByte(pg.GetData())
}

func setVcCloseByte(raw []byte) {
	copy(raw[OF_VC+LEN_VC:OF_VC+LEN_VC+LEN_VC], raw[OF_VC:OF_VC+LEN_VC])
}

func CheckVcPage(pg Page) bool {
	return checkVcByte(pg.GetData())
}

func checkVcByte(raw []byte) bool {
	return bytes.Equal(raw[OF_VC:OF_VC+LEN_VC], raw[OF_VC+LEN_VC:OF_VC+2*LEN_VC])
}

// 管理普通页
func InitRawX() []byte {
	raw := make([]byte, PAGE_SIZE)
	setFSO(raw, OF_DATA)
	return raw
}

func setFSO(raw []byte, ofData uint) {
	binary.BigEndian.PutUint32(raw[OF_FREE:], uint32(ofData))
}

func GetFSO(pg Page) int16 {
	return getFSO(pg.GetData())
}

func getFSO(raw []byte) int16 {
	return int16(binary.BigEndian.Uint16(raw[0:2]))
}

func Insert(pg Page, raw []byte) int16 {
	pg.SetDirty(true)
	offset := getFSO(pg.GetData())
	copy(pg.GetData()[offset:], raw)
	setFSO(pg.GetData(), (uint(offset) + uint(len(raw))))
	return offset
}

func GetFreeSpace(pg Page) int {
	return PAGE_SIZE - int(getFSO(pg.GetData()))
}

func RecoverInsert(pg Page, raw []byte, offset int16) {
	pg.SetDirty(true)
	copy(pg.GetData()[offset:], raw)
	rawFSO := getFSO(pg.GetData())
	if rawFSO < offset+int16(len(raw)) {
		setFSO(pg.GetData(), (uint(offset) + uint(len(raw))))
	}
}

func RecoverUpdate(pg Page, raw []byte, offset int16) {
	pg.SetDirty(true)
	copy(pg.GetData()[offset:], raw)
}
