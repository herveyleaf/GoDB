package dm

import (
	"encoding/binary"
	"os"
	"sync"
)

const (
	SEED        = 13331
	OF_SIZE     = 0
	OF_CHECKSUM = OF_SIZE + 4
	OF_LOG_DATA = OF_CHECKSUM + 4
	LOG_SUFFIX  = ".log"
)

type Logger interface {
	Log(data []byte)
	Truncate(x int64)
	Next() []byte
	Rewind()
	Close()
}

type LoggerImpl struct {
	file      *os.File
	lock      sync.Mutex
	position  int64
	fileSize  int64
	xChecksum int
}

func NewLoggerImpl(file *os.File) *LoggerImpl {
	return &LoggerImpl{
		file: file,
		lock: sync.Mutex{},
	}
}

func NewLoggerImplWithChecksum(file *os.File, xChecksum int) *LoggerImpl {
	return &LoggerImpl{
		file:      file,
		lock:      sync.Mutex{},
		xChecksum: xChecksum,
	}
}

func CreateLogger(path string) (Logger, error) {
	filePath := path + LOG_SUFFIX
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrorFileExists
		}
	} else {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil {
		mode := fi.Mode()
		if mode.Perm()&0400 == 0 || mode.Perm()&0200 == 0 {
			return nil, ErrorFileCannotRW
		} else {
			return nil, err
		}
	}
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, 0)
	f.Seek(0, 0)
	f.Write(data)
	f.Sync()

	return NewLoggerImplWithChecksum(f, 0), nil
}

func OpenLogger(path string) (Logger, error) {
	filePath := path + LOG_SUFFIX
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrorFileNotExists
		}
	} else {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil {
		mode := fi.Mode()
		if mode.Perm()&0400 == 0 || mode.Perm()&0200 == 0 {
			return nil, ErrorFileCannotRW
		} else {
			return nil, err
		}
	}
	lg := NewLoggerImpl(f)
	lg.init()
	return lg
}

func (li *LoggerImpl) init() {
	size := int64(0)
	fi, _ := li.file.Stat()
	size = fi.Size()
	if size < 4 {
		panic("BADLOGFILE")
	}
	// TODO
}
