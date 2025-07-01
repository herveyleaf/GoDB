package dm

import (
	"os"
	"sync"

	"github.com/herveyleaf/GoDB/internal/backend/cache"
	"github.com/herveyleaf/GoDB/pkg/common"
)

const (
	PAGE_SIZE   = 1 << 13
	MEM_MIN_LIM = 10
	DB_SUFFIX   = ".db"
)

type PageCache interface {
	NewPage(initData []byte) int
	GetPage(pgno int) (Page, error)
	Close()
	Release(page Page)
	TruncateByPgno(maxPgno int)
	GetPageNumber() int
	FlushPage(pg Page)
}

type PageCacheImpl struct {
	*cache.AbstractCache[Page]
	file        *os.File
	fileLock    sync.Mutex
	pageNumbers int64
}

func NewPageCacheImpl(file *os.File, maxResource int) (*PageCacheImpl, error) {
	parent := cache.NewAbstractCache[Page](maxResource)
	if maxResource < MEM_MIN_LIM {
		return nil, common.ErrMemTooSmall
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	length := fileInfo.Size()

	return &PageCacheImpl{
		AbstractCache: parent,
		file:          file,
		fileLock:      sync.Mutex{},
		pageNumbers:   length / PAGE_SIZE,
	}, nil
}

func Create(path string, memory int64) (*PageCacheImpl, error) {
	filePath := path + DB_SUFFIX
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		if os.IsExist(err) {
			return nil, common.ErrFileExists
		} else {
			return nil, err
		}
	}

	if fi, err := f.Stat(); err != nil {
		mode := fi.Mode()
		if mode.Perm()&0400 == 0 || mode.Perm()&0200 == 0 {
			return nil, common.ErrFileCannotRW
		} else {
			return nil, err
		}
	}

	return NewPageCacheImpl(f, int(memory/PAGE_SIZE))
}

func Open(path string, memory int64) (*PageCacheImpl, error) {
	filePath := path + DB_SUFFIX
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, common.ErrFileNotExists
		} else {
			return nil, err
		}
	}

	if fi, err := f.Stat(); err != nil {
		mode := fi.Mode()
		if mode.Perm()&0400 == 0 || mode.Perm()&0200 == 0 {
			return nil, common.ErrFileCannotRW
		} else {
			return nil, err
		}
	}

	return NewPageCacheImpl(f, int(memory/PAGE_SIZE))
}

func (pc *PageCacheImpl) NewPage(initData []byte) int {
	pgno := pc.pageNumbers + 1
	pg := NewPageImpl(int(pgno), initData, nil)
	pc.FlushPage(pg)
	return int(pgno)
}

func (pc *PageCacheImpl) GetPage(pgno int) (Page, error) {
	return pc.AbstractCache.Get(int64(pgno))
}

func (pc *PageCacheImpl) GetForCache(key int64) (Page, error) {
	pgno := int(key)
	offset := pageOffset(pgno)
	data := make([]byte, PAGE_SIZE)
	pc.fileLock.Lock()
	defer pc.fileLock.Unlock()
	if _, err := pc.file.ReadAt(data, offset); err != nil {
		return nil, err
	}
	return NewPageImpl(pgno, data, pc), nil
}

func (pc *PageCacheImpl) ReleaseForCache(pg Page) {
	if pg.IsDirty() {
		pc.FlushPage(pg)
		pg.SetDirty(false)
	}
}

func (pc *PageCacheImpl) Release(page Page) {
	pc.AbstractCache.Release(int64(page.GetPageNumber()))
}

func (pc *PageCacheImpl) FlushPage(pg Page) {
	pgno := pg.GetPageNumber()
	offset := pageOffset(pgno)
	pc.fileLock.Lock()
	defer pc.fileLock.Unlock()

	if _, err := pc.file.WriteAt(pg.GetData(), offset); err != nil {
		panic(err)
	}

	if err := pc.file.Sync(); err != nil {
		panic(err)
	}
}

func (pc *PageCacheImpl) TruncateByPgno(maxPgno int) {
	size := pageOffset(maxPgno + 1)
	pc.fileLock.Lock()
	defer pc.fileLock.Unlock()
	pc.file.Truncate(size)
	pc.pageNumbers = int64(maxPgno)
}

func (pc *PageCacheImpl) Close() {
	pc.AbstractCache.Close()
	pc.file.Close()
}

func (pc *PageCacheImpl) GetPageNumber() int {
	return int(pc.pageNumbers)
}

func pageOffset(pgno int) int64 {
	return int64(pgno-1) * PAGE_SIZE
}
