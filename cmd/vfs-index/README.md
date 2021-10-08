vfs-indx command
===================


vfs-index command( ./cmd/vfs-index ) can indexing and searchin from command line.


## usage sample


```bash
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

```console 
$ go get github.com/kazu/vfs-index/cmd/vfs-index
$ vfs-index index --index=../idx --column=name --table=test --data=./
100% |██████████████████████████████████████████████████| (2316/2316, 2072 it/s) [1s:0s]
$
```

## search data 

can search for indexing.




```
$ vfs-index search  -q='name.search("2011_04")' --index=../idx --column=name --table=test --data=./
{"id":130988433,"name":"2011_04_24-1.m4v"}
{"id":130988434,"name":"2011_04_24-2.mp4"}
{"id":130988435,"name":"2011_04_24-3.mp4"}
index merging done [==============================================================] 67507 / 67507
```

### search by number attributes/field 

```
$ vfs-index search  -q='id == 130988433' --index=../idx --column=name --table=test --data=./
{"id":130988433,"name":"2011_04_24-1.m4v"}
```

### merge index

index file was separated by indexing. 

idexing and merging is working on indexing. but indexing dont wait for complete of merging.
so you should merge index.


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