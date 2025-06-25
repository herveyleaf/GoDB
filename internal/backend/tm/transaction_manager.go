package tm

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

const (
	// XID文件头长度
	LEN_XID_HEADER_LENGTH = 8

	// 每个事务的占用长度
	XID_FIELD_SIZE = 1

	// 事务的三种状态
	FIELD_TRAN_ACTIVE    = 0
	FIELD_TRAN_COMMITTED = 1
	FIELD_TRAN_ABORTED   = 2

	// 超级事务，永远为committed状态
	SUPER_XID = 0

	XID_SUFFIX = ".xid"
)

// 错误定义
// 待迁移至统一文件内
var (
	ErrorFileExists    = errors.New("File already exists!")
	ErrorFileCannotRW  = errors.New("File cannot read or write!")
	ErrorFileNotExists = errors.New("File does not exists!")
	ErrorBadXIDFile    = errors.New("Bad XID file!")
	// 新增，待优化
	ErrorInvalidFileAccess = errors.New("Invalid file access!")
)

// TransactionManager接口
type TransactionManager interface {
	Begin() int64
	Commit(xid int64)
	Abort(xid int64)
	IsActive(xid int64) bool
	IsCommitted(xid int64) bool
	IsAborted(xid int64) bool
	Close()
}

type transactionManager struct {
	file        *os.File
	xidCounter  int64
	counterLock sync.Mutex
}

// 创建新的事务管理器
func Create(path string) (TransactionManager, error) {
	filePath := path + XID_SUFFIX

	// 检查文件是否存在
	if _, err := os.Stat(filePath); err == nil {
		return nil, ErrorFileExists
	}

	// 创建文件并设置读写权限
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}

	// 写入空XID文件头
	if _, err := file.Write(make([]byte, LEN_XID_HEADER_LENGTH)); err != nil {
		file.Close()
		return nil, err
	}

	// 确保文件写入磁盘
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, err
	}

	return &transactionManager{
		file:       file,
		xidCounter: 0,
	}, nil
}

// 打开现有的事务管理器
func Open(path string) (TransactionManager, error) {
	filePath := path + XID_SUFFIX

	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, ErrorFileNotExists
	}

	// 打开文件
	file, err := os.OpenFile(filePath, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	// 验证文件有效性
	tm := &transactionManager{file: file}
	if err := tm.checkXIDCounter(); err != nil {
		file.Close()
		return nil, err
	}

	return tm, nil
}

func (tm *transactionManager) checkXIDCounter() error {
	// 获取文件大小
	fileInfo, err := tm.file.Stat()
	if err != nil {
		return err
	}
	fileLen := fileInfo.Size()

	// 文件必须至少包含头部信息
	if fileLen < LEN_XID_HEADER_LENGTH {
		return ErrorBadXIDFile
	}

	// 读取头部计数器
	header := make([]byte, LEN_XID_HEADER_LENGTH)
	if _, err := tm.file.ReadAt(header, 0); err != nil {
		return err
	}

	// 解析计数器
	tm.xidCounter = int64(binary.LittleEndian.Uint64(header))

	// 验证文件大小是否正确
	expectedSize := LEN_XID_HEADER_LENGTH + tm.xidCounter*XID_FIELD_SIZE
	if fileLen != expectedSize {
		return ErrorBadXIDFile
	}

	return nil
}

// 获取事务在文件中的位置
func (tm *transactionManager) getXidPosition(xid int64) int64 {
	return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE
}

// 更新事务状态
func (tm *transactionManager) updateXID(xid int64, status byte) {
	// 计算文件位置
	offset := tm.getXidPosition(xid)

	// 写入状态字节
	tm.file.WriteAt([]byte{status}, offset)

	// 确保数据写入磁盘
	tm.file.Sync()
}

// 增加XID计数器并更新文件头部
func (tm *transactionManager) incrXIDCounter() {
	tm.xidCounter++

	// 将新计数器转换为字节
	buf := make([]byte, LEN_XID_HEADER_LENGTH)
	binary.LittleEndian.PutUint64(buf, uint64(tm.xidCounter))

	// 写入文件头部
	tm.file.WriteAt(buf, 0)

	// 确保数据写入磁盘
	tm.file.Sync()
}

// 开始一个新事务
func (tm *transactionManager) Begin() int64 {
	tm.counterLock.Lock()
	defer tm.counterLock.Unlock()

	// 生成新XID
	newXID := tm.xidCounter + 1

	// 更新事务状态
	tm.updateXID(newXID, FIELD_TRAN_ACTIVE)

	// 增加计数器
	tm.incrXIDCounter()

	return newXID
}

// 提交XID事务
func (tm *transactionManager) Commit(xid int64) {
	if xid == SUPER_XID {
		return
	}
	tm.updateXID(xid, FIELD_TRAN_COMMITTED)
}

// 回滚XID事务
func (tm *transactionManager) Abort(xid int64) {
	if xid == SUPER_XID {
		return
	}
	tm.updateXID(xid, FIELD_TRAN_ABORTED)
}

// 检查事务状态
func (tm *transactionManager) checkXID(xid int64, status byte) bool {
	// 计算文件位置
	offset := tm.getXidPosition(xid)

	// 读取状态字节
	buf := make([]byte, XID_FIELD_SIZE)
	if _, err := tm.file.ReadAt(buf, offset); err != nil {
		panic(ErrorInvalidFileAccess)
	}

	return buf[0] == status
}

func (tm *transactionManager) IsActive(xid int64) bool {
	if xid == SUPER_XID {
		return false
	}
	return tm.checkXID(xid, FIELD_TRAN_ACTIVE)
}

func (tm *transactionManager) IsCommitted(xid int64) bool {
	if xid == SUPER_XID {
		return true
	}
	return tm.checkXID(xid, FIELD_TRAN_COMMITTED)
}

func (tm *transactionManager) IsAborted(xid int64) bool {
	if xid == SUPER_XID {
		return false
	}
	return tm.checkXID(xid, FIELD_TRAN_ABORTED)
}

func (tm *transactionManager) Close() {
	tm.file.Close()
}
