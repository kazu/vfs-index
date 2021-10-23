package vfsindex

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/kazu/fbshelper/query/base"
	"github.com/kazu/vfs-index/query"
)

type GlobRequest struct {
	key string
	val string
}

type GlobCache struct {
	Keys    []string
	ReqCh   chan GlobRequest
	ctx     context.Context
	cancel  context.CancelFunc
	isWork  map[string]bool
	dReader map[string]io.ReaderAt
}

var globCacheInstance GlobCache

func init() {

	// globCacheInstance = GlobCache{
	// 	ReqCh:   make(chan GlobRequest, 512),
	// 	isWork:  map[string]bool{},
	// 	dReader: map[string]io.ReaderAt{},
	// }
	// globCacheInstance.ctx, globCacheInstance.cancel = context.WithCancel(context.Background())
	// globCacheInstance.Start()
}

func (g *GlobCache) Start() error {
	g.ReqCh = make(chan GlobRequest, 512)

	go func() {
		for {
			select {
			case <-g.ctx.Done():
				for k, v := range g.isWork {
					if v {

						g.ReqCh <- GlobRequest{k, ""}
					}
				}
			}
		}
	}()
	return nil
}

func (g GlobCache) Run(key string) {

	defer func() {
		g.isWork[key] = false
	}()
	pat := key
	path := globcachePath(pat)
	dataPath := globcachePath(pat) + ".dat"
	wPath := fmt.Sprintf("%s.%d", path, os.Getpid())
	wDataPath := fmt.Sprintf("%s.%d.dat", path, os.Getpid())

	var root query.Root
	if FileExist(path) {
		if e := SafeRename(path, wPath); e != nil {
			return
		}
		f, e := os.Open(wPath)
		if e != nil {
			return
		}
		buf, _ := ioutil.ReadAll(f)
		f.Close()
		root = query.OpenByBuf(buf)
	} else {
		root = *(query.NewRoot())
		root.WithHeader()
		root.SetVersion(query.FromInt32(1))
	}

	odataf, _ := os.Create(wDataPath)
	defer odataf.Close()
	dataf := NewBufWriterIO(odataf, 8192)

	opathinfos := query.NewPathInfoList()
	pathinfos := base.CommonList{}
	pathinfos.CommonNode = opathinfos.CommonNode
	var idxEntry *query.IdxEntry

	prepare := func(flist *query.PathInfoList, idxEntry *query.IdxEntry, root *query.Root) {
		flist.Merge()
		idxEntry = query.NewIdxEntry()
		idxEntry.SetPathinfos(flist)
		root.SetIndex(&query.Index{CommonNode: idxEntry.CommonNode})
		root.Merge()
	}

	setup := func(cl *base.CommonList, w io.Writer) {
		cl.BaseToNoLayer()
		cl.SetDataWriter(w)
		cl.WriteDataAll()
	}

	after := func(cl *base.CommonList, root *query.Root, r io.ReaderAt) {
		cl.Merge()
		bytes := root.R(0)
		pos := root.Index().IdxEntry().Pathinfos().CommonNode.NodeList.ValueInfo.Pos - 4
		headsize := int(cl.VLen())*4 + 4
		Log(LOG_DEBUG, "GlobCache.Run.after() pos=%d headsize=%d len(bytes)=%d, %d len(cl.R())=%d  \n",
			pos, headsize, root.LenBuf(), len(bytes), len(cl.R(cl.NodeList.ValueInfo.Pos-4)),
		)
		if len(bytes) < pos+headsize {
			nbytes := make([]byte, pos+headsize)
			copy(nbytes, bytes)
			bytes = nbytes
		}

		copy(bytes[pos:pos+headsize], cl.R(cl.NodeList.ValueInfo.Pos - 4)[:headsize])
		bytes = bytes[: pos+headsize : pos+headsize]
		root.IO = base.NewDirectReader(base.NewBase(bytes), r)
	}

	if root.FieldAt(2).Node != nil && root.Index().IdxEntry().Pathinfos().Count() > 0 {
		opathinfos = root.Index().IdxEntry().Pathinfos()
	} else {
		prepare(opathinfos, idxEntry, &root)
	}

	if pathinfos.Count() > 1 {
		setup(&pathinfos, dataf)
	}
	total := 1204
	bar := Pbar.Add(
		fmt.Sprintf("add indexpath %s...", filepath.Base(path)[0:4]),
		total)
	var cnt int
	for {
		req, ok := <-g.ReqCh
		if !ok {
			break
		}

		if req.key != key {
			g.ReqCh <- req
			continue
		}
		if req.val == "" {
			Log(LOG_DEBUG, "GlobCache stop request req=%s  path=%s \n", req.key, globcachePath(pat))
			break
		}
		cnt = pathinfos.Count()
		if cnt == 1 {
			setup(&pathinfos, dataf)
		}

		pathinfo := query.NewPathInfo()
		pathinfo.BaseToNoLayer()
		pathinfo.SetPath(base.FromByteList([]byte(req.val)))
		pathinfo.Merge()
		pathinfos.SetAt(pathinfos.Count(), pathinfo.CommonNode)
		if cnt%8 == 7 {
			Log(LOG_DEBUG, "GlobCache.Run() merge pathinfos  cur=%d \n", cnt)
			//Log(LOG_WARN, "GlobCache.Run() merge pathinfos  cur=%d \n", cnt)
			pathinfos.Merge()
		}
		bar.IncrBy(1)
		if cnt >= (total - 10) {
			total += (total / 2)
			bar.SetTotal(int64(total), false)
		}
		//Log(LOG_DEBUG, "GlobCache.Add() done key=%s val=%s file=%s\n", key, req.val, wDataPath)
	}
	defer func() {
		//bar.SetTotal(int64(cnt), true)
		bar.SetTotal(bar.Current(), true)
		Pbar.wg.Done()
	}()
	dataf.Flush()
	after(&pathinfos, &root, dataf)

	fstat, e := odataf.Stat()
	if e != nil && fstat.Size() < 1 {
		Log(LOG_WARN, "F: data file is zero or error stat=%v error=%s \n",
			fstat, e)
		return
	}

	f, e := os.Create(wPath)
	if e != nil {
		return
	}
	defer f.Close()
	f.Write(root.R(0))

	if e := SafeRename(wPath, path); e != nil {
		os.Remove(wPath)
		Log(LOG_DEBUG, "F: rename %s -> %s \n", wPath, path)
		return
	}
	SafeRename(wDataPath, dataPath)
	Log(LOG_DEBUG, "GlobCache: write pathdata done key=%s len=%d \n", dataPath, pathinfos.Count())

	return
}

func (g *GlobCache) Finish(key string) {
	Log(LOG_DEBUG, "GlobCache.Finsh() stop request reeq=%s len(reqCh)=%d\n", key, len(g.ReqCh))

	g.Add(key, "")

}
func (g *GlobCache) Add(key, value string) {
	if !g.isWork[key] {
		g.isWork[key] = true
		go g.Run(key)
	}
	g.ReqCh <- GlobRequest{key, value}
	//if len(g.ReqCh) > 500 {
	Log(LOG_DEBUG, "GlobCache.Add() len(reqCh)=%d\n", len(g.ReqCh))
	//Log(LOG_WARN, "GlobCache.Add() len(reqCh)=%d\n", len(g.ReqCh))
	//}

}

func (g *GlobCache) has(pat string) bool {
	path := globcachePath(pat)
	if !FileExist(path) {
		return false
	}
	return true
}

func (g *GlobCache) Get(pat string) *query.PathInfoList {

	for {
		if g.isWork[pat] == false && g.has(pat) {
			break
		}
		time.Sleep(1 * time.Second)
	}
	result := g.PrepareRead(pat)
	dpath := globcachePath(pat) + ".dat"

	if g.dReader == nil {
		g.dReader = map[string]io.ReaderAt{}
	}

	if g.dReader[pat] == nil {
		f, e := os.Open(dpath)
		if e != nil {
			return nil
		}
		g.dReader[pat] = f
	}
	result.IO = base.NewDirectReader(result.IO, g.dReader[pat])
	//Log(LOG_DEBUG, "GlobCache.Get() open %s \n", globcachePath(pat))

	return result
}

func (g *GlobCache) GetCh(pat string) <-chan string {
	list := g.Get(pat)
	ch := make(chan string, 100)
	go func() {
		for i := 0; i < list.Count(); i++ {
			info, _ := list.At(i)
			ch <- string(info.Path().Bytes())
		}
	}()
	return ch
}

func (g *GlobCache) PrepareRead(pat string) *query.PathInfoList {
	path := globcachePath(pat)
	dpath := globcachePath(pat) + ".dat"
	f, e := os.Open(path)
	if e != nil {
		return nil
	}
	buf, _ := ioutil.ReadAll(f)
	f.Close()
	f, e = os.Open(dpath)
	if e != nil {
		return nil
	}

	root := query.OpenByBuf(buf)
	return root.Index().IdxEntry().Pathinfos()
	// pathinfos.Base = NewDirectReader(pathinfos.Base, f)

	//return pathinfos
}
