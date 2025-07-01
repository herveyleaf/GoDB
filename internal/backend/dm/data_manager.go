package dm

import (
	"github.com/herveyleaf/GoDB/internal/backend/cache"
	"github.com/herveyleaf/GoDB/internal/backend/tm"
	"github.com/herveyleaf/GoDB/internal/backend/utils"
	"github.com/herveyleaf/GoDB/pkg/common"
)

type DataManager interface {
	Read(uid int64) (DataItem, error)
	Insert(xid int64, data []byte) (int64, error)
	Close()
}

type DataManagerImpl struct {
	tm      tm.TransactionManagerImpl
	pc      PageCache
	logger  Logger
	pIndex  *PageIndex
	pageOne Page

	parent *cache.AbstractCache[DataItemImpl]
}

func NewDataManaerImpl(pc PageCache, logger Logger, tm tm.TransactionManagerImpl) *DataManagerImpl {
	parent := cache.NewAbstractCache[DataItemImpl](0)
	return &DataManagerImpl{
		tm:     tm,
		pc:     pc,
		logger: logger,
		pIndex: NewPageIndex(),
		parent: parent,
	}
}

func CreateDM(path string, mem int64, tm tm.TransactionManagerImpl) DataManager {
	pc, _ := Create(path, mem)
	lg, _ := CreateLogger(path)

	dm := NewDataManaerImpl(pc, lg, tm)
	dm.InitPageOne()
	return dm
}

func OpenDM(path string, mem int64, tm tm.TransactionManagerImpl) DataManager {
	pc, _ := Open(path, mem)
	lg, _ := OpenLogger(path)
	dm := NewDataManaerImpl(pc, lg, tm)
	if !dm.LoadCheckPageOne() {
		Recover(&tm, lg, pc)
	}
	dm.FillPageIndex()
	SetVcOpenPage(dm.pageOne)
	dm.pc.FlushPage(dm.pageOne)

	return dm
}

func (dm *DataManagerImpl) Read(uid int64) (DataItem, error) {
	newdi, err := dm.parent.Get(uid)
	if !newdi.IsValid() {
		newdi.Release()
		return nil, err
	}
	return &newdi, nil
}

func (dm *DataManagerImpl) Insert(xid int64, data []byte) (int64, error) {
	raw := WrapDataItemRaw(data)
	if len(raw) > MAX_FREE_SPACE {
		panic(common.ErrDataTooLarge)
	}

	var pi PageInfo = PageInfo{}
	for i := 0; i < 5; i++ {
		pi = dm.pIndex.Select(len(raw))
		if pi != (PageInfo{}) {
			break
		} else {
			newPgno := dm.pc.NewPage(InitRawO())
			dm.pIndex.Add(newPgno, MAX_FREE_SPACE)
		}
	}
	if pi == (PageInfo{}) {
		panic(common.ErrDatabaseBusy)
	}

	var pg Page = nil
	freeSpace := 0
	defer func() {
		if pg != nil {
			dm.pIndex.Add(pi.Pgno, GetFreeSpace(pg))
		} else {
			dm.pIndex.Add(pi.Pgno, freeSpace)
		}
	}()
	pg, _ = dm.pc.GetPage(pi.Pgno)
	log := InsertLog(xid, pg, raw)
	dm.logger.Log(log)
	offset := Insert(pg, raw)
	pg.Release()
	return utils.AddressToUid(pi.Pgno, offset), nil
}

func (dm *DataManagerImpl) Close() {
	dm.parent.Close()
	dm.logger.Close()

	SetVcClosePage(dm.pageOne)
	dm.pageOne.Release()
	dm.pc.Close()
}

func (dm *DataManagerImpl) LogDataItem(xid int64, di DataItem) {
	log := UpdateLog(xid, di)
	dm.logger.Log(log)
}

func (dm *DataManagerImpl) ReleaseDataItem(di DataItem) {
	dm.parent.Release(di.GetUid())
}

func (dm *DataManagerImpl) GetForCache(uid int64) DataItem {
	offset := int16(uid & ((int64(1) << 32) - 1))
	uid = int64(uint64(uid) >> 32)
	pgno := int(uid & ((int64(1) << 32) - 1))
	pg, _ := dm.pc.GetPage(pgno)
	return ParseDataItem(pg, offset, *dm)
}

func (dm *DataManagerImpl) ReleaseForCache(di DataItem) {
	di.Page().Release()
}

func (dm *DataManagerImpl) InitPageOne() {
	pgno := dm.pc.NewPage(InitRawO())
	if pgno != 1 {
		panic("pgno must be 1")
	}
	dm.pageOne, _ = dm.pc.GetPage(pgno)
	dm.pc.FlushPage(dm.pageOne)
}

func (dm *DataManagerImpl) LoadCheckPageOne() bool {
	dm.pageOne, _ = dm.pc.GetPage(1)
	return CheckVcPage(dm.pageOne)
}

func (dm *DataManagerImpl) FillPageIndex() {
	pageNumber := dm.pc.GetPageNumber()
	for i := 2; i <= pageNumber; i++ {
		var pg Page = nil
		pg, _ = dm.pc.GetPage(i)
		dm.pIndex.Add(pg.GetPageNumber(), GetFreeSpace(pg))
		pg.Release()
	}
}
