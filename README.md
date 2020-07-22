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
$ vfs-index index search  --index=../idx --column=name --table=test --data=./
100% |██████████████████████████████████████████████████| (2316/2316, 2072 it/s) [1s:0s]
$
```

search data 

```
$ vfs-index search  -q="2011_04" --index=../idx --column=name --table=test --data=./
{"id":130988433,"name":"2011_04_24-1.m4v"}
{"id":130988434,"name":"2011_04_24-2.mp4"}
{"id":130988435,"name":"2011_04_24-3.mp4"}
index merging done [==============================================================] 67507 / 67507
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
- [ ] csv support 
- [ ] msgpack support
- [ ] support compression data (  lz4(?) )