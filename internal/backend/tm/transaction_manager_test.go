package tm

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/herveyleaf/GoDB/pkg/common"
)

func TestCreateAndOpen(t *testing.T) {
	dir, err := os.MkdirTemp("D:\\temp", "tm_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test")
	tm1, err := Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	defer tm1.Close()

	// 验证文件创建
	if _, err := os.Stat(path + XID_SUFFIX); os.IsNotExist(err) {
		t.Fatal("XID file not created")
	}

	// 测试重复创建错误
	if _, err := Create(path); err != common.ErrFileExists {
		t.Fatalf("Expected ErrorFileExists, got %v", err)
	}

	// 测试打开
	tm2, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer tm2.Close()

	// 测试打开不存在的文件
	if _, err := Open(filepath.Join(dir, "nonexistent")); err != common.ErrFileNotExists {
		t.Fatalf("Expected ErrorFileNotExists, got %v", err)
	}
}

func TestSuperXID(t *testing.T) {
	tm := &TransactionManagerImpl{} // 用于测试状态方法

	if !tm.IsCommitted(SUPER_XID) {
		t.Fatal("SUPER_XID should be committed")
	}
	if tm.IsActive(SUPER_XID) || tm.IsAborted(SUPER_XID) {
		t.Fatal("SUPER_XID should not be active or aborted")
	}
}

func TestErrorCases(t *testing.T) {
	dir, _ := os.MkdirTemp("", "tm_test")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test")

	// 创建损坏的XID文件
	corruptPath := filepath.Join(dir, "corrupt")
	os.WriteFile(corruptPath+XID_SUFFIX, []byte{0, 0, 0}, 0600)
	if _, err := Open(corruptPath); err != common.ErrBadXIDFile {
		t.Fatalf("Expected ErrorBadXIDFile, got %v", err)
	}

	// 测试无效文件访问
	tm, _ := Create(path)
	tm.Close() // 关闭后操作应失败

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic after close")
		}
	}()
	tm.IsActive(1) // 应触发panic
}

func TestConcurrentBegin(t *testing.T) {
	dir, _ := os.MkdirTemp("", "tm_test")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test")

	tm, _ := Create(path)
	defer tm.Close()

	// 并发开始多个事务
	const n = 100
	ch := make(chan int64, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ch <- tm.Begin()
		}()
	}
	wg.Wait()
	close(ch)

	// 收集所有生成的xid
	xids := make(map[int64]bool)
	for xid := range ch {
		if xids[xid] {
			t.Fatalf("Duplicate xid: %d", xid)
		}
		xids[xid] = true
	}

	// 验证事务数量
	if len(xids) != n {
		t.Fatalf("Expected %d transactions, got %d", n, len(xids))
	}

	// 验证事务状态
	for xid := range xids {
		if !tm.IsActive(xid) {
			t.Fatalf("Transaction %d should be active", xid)
		}
	}
}

func TestFileOperations(t *testing.T) {
	dir, _ := os.MkdirTemp("", "tm_test")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test")

	// 创建并立即关闭
	tm1, _ := Create(path)
	tm1.Close()

	// 重新打开检查计数器
	tm2, _ := Open(path)

	// 开始事务
	xid := tm2.Begin()
	tm2.Commit(xid)
	tm2.Close()

	// 再次打开验证状态
	tm3, _ := Open(path)
	defer tm3.Close()

	if !tm3.IsCommitted(xid) {
		t.Fatal("Committed transaction not persisted")
	}
}

func TestInvalidStateReading(t *testing.T) {
	dir, _ := os.MkdirTemp("", "tm_test")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "test")

	tm, _ := Create(path)
	defer tm.Close()

	// 获取一个无效xid
	invalidXid := int64(9999999)

	// 测试读取无效状态应触发panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic for invalid xid position")
		}
	}()
	tm.IsActive(invalidXid)
}
