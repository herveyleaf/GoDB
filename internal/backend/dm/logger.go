package dm

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/herveyleaf/GoDB/internal/backend/utils"
	"github.com/herveyleaf/GoDB/pkg/common"
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
			return nil, common.ErrFileExists
		}
	} else {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil {
		mode := fi.Mode()
		if mode.Perm()&0400 == 0 || mode.Perm()&0200 == 0 {
			return nil, common.ErrFileCannotRW
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
			return nil, common.ErrFileNotExists
		}
	} else {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil {
		mode := fi.Mode()
		if mode.Perm()&0400 == 0 || mode.Perm()&0200 == 0 {
			return nil, common.ErrFileCannotRW
		} else {
			return nil, err
		}
	}
	lg := NewLoggerImpl(f)
	lg.init()
	return lg, nil
}

func (li *LoggerImpl) init() {
	size := int64(0)
	fi, _ := li.file.Stat()
	size = fi.Size()
	if size < 4 {
		panic("BADLOGFILE")
	}
	raw := make([]byte, 4)
	_, err := li.file.ReadAt(raw, 0)
	if err != nil {
		panic(err)
	}
	xChecksum := int(binary.BigEndian.Uint32(raw))
	li.fileSize = size
	li.xChecksum = xChecksum

	li.checkAndRemoveTail()
}

func (li *LoggerImpl) checkAndRemoveTail() {
	li.Rewind()

	xCheck := 0
	for {
		log := li.internNext()
		if log == nil {
			break
		}
		xCheck = li.calChecksum(xCheck, log)
	}
	if xCheck != li.xChecksum {
		panic("BADLOGFILE")
	}
	li.Truncate(li.position)
	li.file.Seek(li.position, 0)
	li.Rewind()
}

func (li *LoggerImpl) calChecksum(xCheck int, log []byte) int {
	for _, b := range log {
		xCheck = xCheck*SEED + int(b)
	}
	return xCheck
}

func (li *LoggerImpl) Log(data []byte) {
	log := li.warpLog(data)
	li.lock.Lock()
	defer li.lock.Unlock()
	_, err := li.file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	_, err = li.file.Write(log)
	if err != nil {
		panic(err)
	}
	li.updateXChecksm(log)
}

func (li *LoggerImpl) updateXChecksm(log []byte) {
	li.xChecksum = li.calChecksum(li.xChecksum, log)
	_, err := li.file.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(li.xChecksum))
	n, err := li.file.Write(buf)
	if err != nil {
		if n != len(buf) {
			panic("INCOMPLETEWRITE")
		}
		panic(err)
	}
	li.file.Sync()
}

func (li *LoggerImpl) warpLog(data []byte) []byte {
	checksum := utils.Int2Byte(li.calChecksum(0, data))
	size := utils.Int2Byte(len(data))
	return append(append(size, checksum...), data...)
}

func (li *LoggerImpl) Truncate(x int64) {
	li.lock.Lock()
	defer li.lock.Unlock()

	li.file.Truncate(x)
}

func (li *LoggerImpl) internNext() []byte {
	if li.position+OF_LOG_DATA >= li.fileSize {
		return nil
	}
	tmp := make([]byte, 4)
	_, err := li.file.ReadAt(tmp, li.position)
	if err != nil {
		panic(err)
	}
	size := utils.ParseInt(tmp)
	if li.position+int64(size)+OF_LOG_DATA > li.fileSize {
		return nil
	}
	buf := make([]byte, OF_LOG_DATA+size)
	_, err = li.file.ReadAt(buf, li.position)
	if err != nil {
		panic(err)
	}
	log := buf
	checkSum1 := li.calChecksum(0, log[OF_LOG_DATA:])
	checkSum2 := utils.ParseInt(log[OF_CHECKSUM:OF_LOG_DATA])
	if checkSum1 != checkSum2 {
		return nil
	}
	li.position += int64(len(log))
	return log
}

func (li *LoggerImpl) Next() []byte {
	li.lock.Lock()
	defer li.lock.Unlock()

	log := li.internNext()
	if log == nil {
		return nil
	}
	return log[OF_LOG_DATA:]
}

func (li *LoggerImpl) Rewind() {
	li.position = 4
}

func (li *LoggerImpl) Close() {
	li.file.Close()
}
