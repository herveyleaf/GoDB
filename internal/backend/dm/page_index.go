package dm

import (
	"sync"
)

const (
	INTERVALS_NO int = 40
	THRESHOLD    int = PAGE_SIZE / INTERVALS_NO
)

type PageIndex struct {
	lock  sync.Mutex
	lists [][]PageInfo
}

func NewPageIndex() *PageIndex {
	tmp := make([][]PageInfo, INTERVALS_NO+1)
	for i := 0; i < INTERVALS_NO+1; i++ {
		tmp[i] = make([]PageInfo, 0)
	}
	return &PageIndex{
		lock:  sync.Mutex{},
		lists: tmp,
	}
}

func (pidx *PageIndex) Add(pgno int, freeSpace int) {
	pidx.lock.Lock()
	defer pidx.lock.Unlock()
	number := freeSpace / THRESHOLD
	pidx.lists[number] = append(pidx.lists[number], NewPageInfo(pgno, freeSpace))
}

func (pidx *PageIndex) Select(spaceSize int) PageInfo {
	pidx.lock.Lock()
	defer pidx.lock.Unlock()
	number := spaceSize / THRESHOLD
	if number < INTERVALS_NO {
		number++
	}
	for number <= INTERVALS_NO {
		if len(pidx.lists[number]) == 0 {
			number++
			continue
		}
		page := pidx.lists[number][0]
		pidx.lists[number] = pidx.lists[number][1:]
		return page
	}
	return PageInfo{}
}
