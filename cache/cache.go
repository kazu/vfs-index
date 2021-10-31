package cache

import (
	"sync"

	query2 "github.com/kazu/vfs-index/query"
)

const DEBUG = true

var Global *Cacher = New()
var Counter *CountCache = CountNew()

type resultCache map[string]map[uint64][]*query2.Record
type countCache map[string]map[uint64]int

type Cacher struct {
	enable bool
	mu     sync.Mutex
	Data   resultCache
}

type CountCache struct {
	enable bool
	mu     sync.Mutex
	data   countCache
}

func New() *Cacher {

	c := &Cacher{enable: false}
	c.Reset()
	return c
}

func CountNew() *CountCache {

	c := &CountCache{enable: true}
	c.Reset()
	return c
}

func (c *Cacher) Reset() {
	c.mu.Lock()
	c.Data = map[string]map[uint64][]*query2.Record{}
	c.mu.Unlock()
}

func (c *CountCache) Reset() {
	c.mu.Lock()
	c.data = map[string]map[uint64]int{}
	c.mu.Unlock()
}

func (c *Cacher) Cache(cn string, k uint64, v []*query2.Record) []*query2.Record {
	if !c.enable {
		return nil
	}
	c.mu.Lock()
	if c.Data == nil {
		c.mu.Unlock()
		c.Reset()
		c.mu.Lock()
	}
	if c.Data[cn] == nil {
		c.Data[cn] = map[uint64][]*query2.Record{}
	}

	c.mu.Unlock()

	c.mu.Lock()
	ov := c.Data[cn][k]
	c.mu.Unlock()
	if ov == nil || v != nil {
		goto SET
	}

	return ov

SET:
	c.mu.Lock()
	c.Data[cn][k] = v
	result := c.Data[cn][k]
	c.mu.Unlock()
	return result

}
func (c *CountCache) Cache(cn string, k uint64, v int) int {
	if !c.enable {
		return 0
	}
	c.mu.Lock()
	if c.data == nil {
		c.mu.Unlock()
		c.Reset()
		c.mu.Lock()
	}
	if c.data[cn] == nil {
		c.data[cn] = map[uint64]int{}
	}

	c.mu.Unlock()

	c.mu.Lock()
	ov := c.data[cn][k]
	c.mu.Unlock()
	if v != 0 {
		goto SET
	}

	return ov

SET:
	c.mu.Lock()
	c.data[cn][k] = v
	result := c.data[cn][k]
	c.mu.Unlock()
	return result

}

func (c *CountCache) Raw() map[string]map[uint64]int {
	if !DEBUG {
		return nil
	}
	c.mu.Lock()
	data := c.data
	c.mu.Unlock()
	return data

}
