package cache

import (
	"errors"
	"sync"
)

var ErrCacheFull = errors.New("Cache is full!")

type AbstractCache[T any] struct {
	cache      map[uint64]T        // 实际缓存的数据
	references map[uint64]int      // 元素的引用个数
	getting    map[uint64]struct{} // 正在获取某资源的线程

	maxResource int // 缓存的最大缓存资源数
	count       int // 缓存中元素的个数
	lock        sync.Mutex
	cond        *sync.Cond // 条件变量，用于等待getting中的资源释放
}

func NewAbstractCache[T any](maxCount int) *AbstractCache[T] {
	c := &AbstractCache[T]{
		maxResource: maxCount,
		cache:       make(map[uint64]T),
		references:  make(map[uint64]int),
		getting:     make(map[uint64]struct{}),
	}
	c.cond = sync.NewCond(&c.lock)
	return c
}

func (ac *AbstractCache[T]) Get(key uint64) (T, error) {
	ac.lock.Lock()
	defer ac.lock.Unlock()

	// 等待其它goroutine完成获取
	for _, exists := ac.getting[key]; exists; _, exists = ac.getting[key] {
		ac.cond.Wait()
	}

	// 检查缓存是否存在
	if obj, exists := ac.cache[key]; exists {
		ac.references[key]++
		return obj, nil
	}

	// 检查缓存容量
	if ac.maxResource > 0 && ac.count >= ac.maxResource {
		var zero T
		return zero, ErrCacheFull
	}

	// 标记正在获取
	ac.getting[key] = struct{}{}
	ac.count++

	// 释放锁进行IO操作
	ac.lock.Unlock()
	obj, err := ac.GetForCache(key)
	ac.lock.Lock()

	delete(ac.getting, key)
	if err != nil {
		ac.count--
		ac.cond.Broadcast()
		return obj, err
	}

	// 成功获取后加入缓存
	ac.cache[key] = obj
	ac.references[key] = 1
	ac.cond.Broadcast()
	return obj, nil
}

func (ac *AbstractCache[T]) Release(key uint64) {
	ac.lock.Lock()
	defer ac.lock.Unlock()

	if ref, exists := ac.references[key]; exists {
		ref--
		if ref == 0 {
			obj := ac.cache[key]
			ac.ReleaseForCache(obj)
			delete(ac.references, key)
			delete(ac.cache, key)
			ac.count--
		} else {
			ac.references[key] = ref
		}
	}
}

func (ac *AbstractCache[T]) Close() {
	ac.lock.Lock()
	defer ac.lock.Unlock()

	for key, obj := range ac.cache {
		ac.ReleaseForCache(obj)
		delete(ac.cache, key)
		delete(ac.references, key)
	}
	ac.count = 0
}

// 抽象方法
func (ac *AbstractCache[T]) GetForCache(key uint64) (T, error) {
	panic("GetForCache must be implemented")
}

func (ac *AbstractCache[T]) ReleaseForCache(obj T) {
	panic("ReleaseForCache must be implemented")
}
