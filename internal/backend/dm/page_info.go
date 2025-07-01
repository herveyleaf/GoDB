package dm

type PageInfo struct {
	Pgno      int
	FreeSpace int
}

func NewPageInfo(pgno int, freeSpace int) PageInfo {
	return PageInfo{
		Pgno:      pgno,
		FreeSpace: freeSpace,
	}
}
