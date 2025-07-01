package dm

import (
	"fmt"

	"github.com/herveyleaf/GoDB/internal/backend/tm"
	"github.com/herveyleaf/GoDB/internal/backend/utils"
)

const (
	LOG_TYPE_INSERT byte = 0
	LOG_TYPE_UPDATE byte = 1
	REDO            int  = 0
	UNDO            int  = 1

	OF_TYPE       int = 0
	OF_XID        int = OF_TYPE + 1
	OF_UPDATE_UID int = OF_XID + 8
	OF_UPDATE_RAW int = OF_UPDATE_UID + 8

	OF_INSERT_PGNO   int = OF_XID + 8
	OF_INSERT_OFFSET int = OF_INSERT_PGNO + 4
	OF_INSERT_RAW        = OF_INSERT_OFFSET + 2
)

type InsertLogInfo struct {
	xid    int64
	pgno   int
	offset int16
	raw    []byte
}

type UpdateLogInfo struct {
	xid    int64
	pgno   int
	offset int16
	oldRaw []byte
	newRaw []byte
}

func NewInsertLogInfo() *InsertLogInfo {
	return &InsertLogInfo{}
}

func NewUpdateLogInfo() *UpdateLogInfo {
	return &UpdateLogInfo{}
}

func Recover(tm tm.TransactionManager, lg Logger, pc PageCache) {
	fmt.Println("Recovering...")

	lg.Rewind()
	maxPgno := 0
	for {
		log := lg.Next()
		if log == nil {
			break
		}
		var pgno int
		if isInsertLog(log) {
			li := parseInsertLog(log)
			pgno = li.pgno
		} else {
			li := parseUpdateLog(log)
			pgno = li.pgno
		}
		if pgno > maxPgno {
			maxPgno = pgno
		}
	}
	if maxPgno == 0 {
		maxPgno = 1
	}
	pc.TruncateByPgno(maxPgno)
	fmt.Println("Truncate to ", maxPgno, " pages.")
	redoTransactions(tm, lg, pc)
	fmt.Println("Redo Transactions Over.")
	undoTransactions(tm, lg, pc)
	fmt.Println("Undo Transactions Over")

	fmt.Println("Recovery Over.")
}

func redoTransactions(tm tm.TransactionManager, lg Logger, pc PageCache) {
	lg.Rewind()
	for {
		log := lg.Next()
		if log == nil {
			break
		}
		if isInsertLog(log) {
			li := parseInsertLog(log)
			xid := li.xid
			if !tm.IsActive(xid) {
				doInsertLog(pc, log, REDO)
			}
		} else {
			xi := parseUpdateLog(log)
			xid := xi.xid
			if !tm.IsActive(xid) {
				doUpdateLog(pc, log, REDO)
			}
		}
	}
}

func undoTransactions(tm tm.TransactionManager, lg Logger, pc PageCache) {
	logCache := make(map[int64][][]byte)
	lg.Rewind()
	for {
		log := lg.Next()
		if log == nil {
			break
		}
		if isInsertLog(log) {
			li := parseInsertLog(log)
			xid := li.xid
			if tm.IsActive(xid) {
				if _, exists := logCache[xid]; !exists {
					logCache[xid] = [][]byte{}
				}
				logCache[xid] = append(logCache[xid], log)
			}
		}
	}

	for xid, logs := range logCache {
		for i := len(logs) - 1; i >= 0; i-- {
			log := logs[i]
			if isInsertLog(log) {
				doInsertLog(pc, log, UNDO)
			} else {
				doUpdateLog(pc, log, UNDO)
			}
		}
		tm.Abort(xid)
	}
}

func isInsertLog(log []byte) bool {
	return log[0] == LOG_TYPE_INSERT
}

func UpdateLog(xid int64, di DataItem) []byte {
	logType := []byte{LOG_TYPE_UPDATE}
	xidRaw := utils.Long2Byte(xid)
	uidRaw := utils.Long2Byte(di.GetUid())
	oldRaw := di.GetOldRaw()
	newRaw := di.GetRaw()
	return append(append(append(append(logType, xidRaw...), uidRaw...), oldRaw...), newRaw...)
}

func parseUpdateLog(log []byte) *UpdateLogInfo {
	li := NewUpdateLogInfo()
	li.xid = utils.ParseLong(log[OF_XID:OF_UPDATE_UID])
	uid := utils.ParseLong(log[OF_UPDATE_UID:OF_UPDATE_RAW])
	li.offset = int16(uid & ((int64(1) << 16) - 1))
	uid = int64(uint64(uid) >> 32)
	li.pgno = int(uid & ((int64(1) << 32) - 1))
	length := (len(log) - OF_UPDATE_RAW) / 2
	li.oldRaw = log[OF_UPDATE_RAW : OF_UPDATE_RAW+length]
	li.newRaw = log[OF_UPDATE_RAW+length : OF_UPDATE_RAW+length*2]
	return li
}

func doUpdateLog(pc PageCache, log []byte, flag int) {
	var pgno int
	var offset int16
	var raw []byte
	if flag == REDO {
		xi := parseUpdateLog(log)
		pgno = xi.pgno
		offset = xi.offset
		raw = xi.newRaw
	} else {
		xi := parseUpdateLog(log)
		pgno = xi.pgno
		offset = xi.offset
		raw = xi.oldRaw
	}
	var pg Page = nil
	var err error
	if pg, err = pc.GetPage(pgno); err != nil {
		panic(err)
	}
	defer pg.Release()
	RecoverUpdate(pg, raw, offset)
}

func InsertLog(xid int64, pg Page, raw []byte) []byte {
	logTypeRaw := []byte{LOG_TYPE_INSERT}
	xidRaw := utils.Long2Byte(xid)
	pgnoRaw := utils.Int2Byte(pg.GetPageNumber())
	offsetRaw := utils.Short2Byte(GetFSO(pg))
	return append(append(append(append(logTypeRaw, xidRaw...), pgnoRaw...), offsetRaw...), raw...)
}

func parseInsertLog(log []byte) *InsertLogInfo {
	li := NewInsertLogInfo()
	li.xid = utils.ParseLong(log[OF_XID:OF_INSERT_PGNO])
	li.pgno = utils.ParseInt(log[OF_INSERT_PGNO:OF_INSERT_OFFSET])
	li.offset = utils.ParseShort(log[OF_INSERT_PGNO:OF_INSERT_OFFSET])
	li.raw = log[OF_INSERT_RAW:]
	return li
}

func doInsertLog(pc PageCache, log []byte, flag int) {
	li := parseInsertLog(log)
	var pg Page = nil
	var err error
	if pg, err = pc.GetPage(li.pgno); err != nil {
		panic(err)
	}
	defer pg.Release()
	if flag == UNDO {
		SetDataItemRawInvalid(li.raw)
	}
	RecoverInsert(pg, li.raw, li.offset)
}
