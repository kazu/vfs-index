vfs-index
===================

[![Go test](https://github.com/kazu/vfs-index/actions/workflows/go.yml/badge.svg?branch=master&event=push)](https://github.com/kazu/vfs-index/actions/workflows/go.yml)

[![pkg.go.dev](https://pkg.go.dev/github.com/kazu/vfs-index)](https://pkg.go.dev/github.com/kazu/vfs-index)

vfs-index is simple indexer for simple data collection(json , csv, msgpack...) on OS's virtual file system.
this indexer is not requred daemon process, no lock. 


## use on command lines

- [cmd/vfs-index](cmd/vfs-index): indexing/merging/searching in terminal.

## operantion in golang

this data is bigger ( 100 milion row ?..)
if you search jq, very slow.

vfs-index indexing these files in big collection data(csv, json ...).
vfs-index can search wihtout locking in indexing.
if indexing stop by ctrl-c , indexing will continue on run next index command.


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



## indexing

vfs-index detect data format by file extension.
and support lz4 compression

- `.json`
- `.csv`
- `.json.lz4`
- `.csv.lz4`

support JSONL format in .json.




```go
func DefaultOption() vfs.Option {
	return vfs.RootDir("/Users/xtakei/vfs-idx")
}


idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())
e = idx.Regist("test", "id")
```


## search data 

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
```

search by number attributes/field 

```go


// search by matching substring
sCondName := idx.On("test", vfs.ReaderColumn("name"), vfs.Output(vfs.MapInfOutput))
matches2 := sCondName.Match("ロシア人").All()
```





## merge index

merginge stop after 1 minutes.

```go

idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())
sCond := idx.On("test", 
  vfs.ReaderColumn("id"), 
  vfs.Output(vfs.MapInfOutput), 
  vfs.MergeDuration(time.Second*time.Duration(60)))

sCond.StartMerging()
sCond.CancelAndWait()
```

## support query.

if attribute/field is  number, can select by Arithmetic comparison.
support comparation ops. == >= < <= > .

```go
idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())

sCond := idx.On("test", vfs.Output(vfs.MapInfOutput))

results := sCond.Query(qstr).All(`title.search("鬼滅の") && id == 3365460`)
```

## saerch with Match() 

```go
idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())
sCond := idx.On("test", vfs.ReaderColumn("title"), vfs.Output(vfs.MapInfOutput))
matches := sCond.Match("鬼滅").All()
```

## search with selector( Select() function)


```go
idx, e := vfs.Open("/Users/xtakei/example/data", DefaultOption())

sCond := idx.On("test", vfs.ReaderColumn("title"), vfs.Output(vfs.MapInfOutput))

matches := sCond.Select(func(cond vfs.SearchElem) bool {
		return cond.Op("title", "==", "警視庁")
}).All()
```

## output filter on searching
Josn output

```go
jsonstr := sCond.Select(func(cond vfs.SearchElem) bool {
		return cond.Op("title", "==", "警視庁")
}).All(vfs.ResultOutput("json"))

```

csv output


```go
csvstr := sCond.Select(func(cond vfs.SearchElem) bool {
		return cond.Op("title", "==", "警視庁")
}).All(vfs.ResultOutput("csv"))

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
- [x] support compression data (  lz4(?) )
- [x] binary search mergedindex file
- [ ] improve performance to search Comparison operator(exclude equal) with stream
  - [x] improve performance to search in merged index
  - [x] improve performance to search in non-merged index