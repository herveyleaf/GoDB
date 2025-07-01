package common

import "errors"

// 通用错误
var (
	ErrCacheFull     = errors.New("cache is full")
	ErrFileExists    = errors.New("file already exists")
	ErrFileNotExists = errors.New("file does not exist")
	ErrFileCannotRW  = errors.New("file cannot read or write")
)

// 数据管理器(DM)错误
var (
	ErrBadLogFile   = errors.New("bad log file")
	ErrMemTooSmall  = errors.New("memory too small")
	ErrDataTooLarge = errors.New("data too large")
	ErrDatabaseBusy = errors.New("database is busy")
)

// 事务管理器(TM)错误
var (
	ErrBadXIDFile = errors.New("bad XID file")
)

// 版本管理器(VM)错误
var (
	ErrDeadlock         = errors.New("deadlock")
	ErrConcurrentUpdate = errors.New("concurrent update issue")
	ErrNullEntry        = errors.New("null entry")
)

// 表管理器(TBM)错误
var (
	ErrInvalidField    = errors.New("invalid field type")
	ErrFieldNotFound   = errors.New("field not found")
	ErrFieldNotIndexed = errors.New("field not indexed")
	ErrInvalidLogOp    = errors.New("invalid logic operation")
	ErrInvalidValues   = errors.New("invalid values")
	ErrDuplicatedTable = errors.New("duplicated table")
	ErrTableNotFound   = errors.New("table not found")
)

// 解析器错误
var (
	ErrInvalidCommand = errors.New("invalid command")
	ErrTableNoIndex   = errors.New("table has no index")
)

// 传输层错误
var (
	ErrInvalidPkgData = errors.New("invalid package data")
)

// 服务器错误
var (
	ErrNestedTransaction = errors.New("nested transaction not supported")
	ErrNoTransaction     = errors.New("not in transaction")
)

// 启动器错误
var (
	ErrInvalidMem = errors.New("invalid memory")
)
