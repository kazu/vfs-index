vfs-index
===================

vfs-index is simple indexer for simple data collection(json , csv, msgpack...) on OS's virtual file system.
this indexer is not requred daemon process, no lock. 


```console
$ cat test.json
[
  {
    "id": 130988433,
    "name": "2011_04_24-1.m4v"
  },
  {
    "id": 130988434,
    "name": "2011_04_24-2.mp4"
  },
  {
    "id": 130988435,
    "name": "2011_04_24-3.mp4"
  },

```

this data is bigger ( 100 milion row ?..)
if you search jq, very slow.

indexing this data. if indexing stop by ctrl-c , indexing will continue on run next index command.

```console 
$ go get github.com/kazu/vfs-index/cmd/vfs-index
$ vfs-index index --index=../idx --column=name --table=test --data=./
100% |██████████████████████████████████████████████████| (2316/2316, 2072 it/s) [1s:0s]
$
```

search data 

```
$ vfs-index search  -q='name.search("2011_04")' --index=../idx --column=name --table=test --data=./
{"id":130988433,"name":"2011_04_24-1.m4v"}
{"id":130988434,"name":"2011_04_24-2.mp4"}
{"id":130988435,"name":"2011_04_24-3.mp4"}
index merging done [==============================================================] 67507 / 67507
```

search by number attributes/field 

```
$ vfs-index search  -q='id == 130988433' --index=../idx --column=name --table=test --data=./
{"id":130988433,"name":"2011_04_24-1.m4v"}
```

merge index

```
$ vfs-index merge --index=../idx --column=name --table=test --data=./
index merging done [==============================================================] 67507 / 67507
```

## support query.

if attribute/field is  number, can select by Arithmetic comparison.
support comparation ops. == >= < <= > .

```
$ vfs-index search -q='id == 130988471' --index=../idx --table=test --data=./
```

string search 
```
$ vfs-index search -q='name.search("フロンターレ")' --index=../idx --table=test --data=./
```


## use in golang.

import package 

```go
import vfs "github.com/kazu/vfs-index"
```


indexing 

```go
func DefaultOption() vfs.Option {
	return vfs.RootDir("/Users/xtakei/vfs-idx")
}


idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())
e = idx.Regist("test", "id")
```

searching 
```go
func DefaultOption() vfs.Option {
	return vfs.RootDir("/Users/xtakei/vfs-idx")
}

// search number index
idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())
sCond := idx.On("test", vfs.ReaderColumn("id"), vfs.Output(vfs.MapInfOutput))

record := sCond.Select(func(m vfs.SearchCondElem2) bool {
    return m.Op("id", "<",  122878513)
}).First()

// search by matching substring
sCondName := idx.On("test", vfs.ReaderColumn("name"), vfs.Output(vfs.MapInfOutput))
matches2 := sCondName.Match("ロシア人").All()

```


index merging
stop after 1 minutes.

```go

idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())
sCond := idx.On("test", vfs.ReaderColumn("id"), vfs.Output(vfs.MapInfOutput))
sCond.StartMerging()
time.Sleep(1 * time.Minute)
sCond.CancelAndWait()
```



## TODO

- [x] write file list
- [x] read file list 
- [x] write number column index
- [x] read number column index
- [x] write tri-gram clumn index
- [x] support string search
- [x] support index merging
- [x] add comment
- [x] csv support 
- [ ] msgpack support
- [ ] support compression data (  lz4(?) )