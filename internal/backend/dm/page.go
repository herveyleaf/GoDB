package dm

type Page interface {
	Lock()
	Unlock()
	Release()
	SetDirty(dirty bool)
	IsDirty() bool
	GetPageNumber() int
	GetData() []byte
}
