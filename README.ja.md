vfs-index
===================

vfs-index は簡単なVFS上の json/csv のようなデータの検索インデクサです。 db のようにindex を作成して検索をしますが、index には
ロックはなく、インデックスが途中で停止しても、次回実行時に継続されます。非常にレコードの多いようなtable 型のデータ場合jq 等で検索するのは
非常遅いです。


以下のようなデータで
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

index サブコマンドでindex 登録します。登録せずに search 時にも可能です。

```console 
$ go get github.com/kazu/vfs-index/cmd/vfs-index
$ vfs-index index  --index=../idx --column=name --table=test --data=./
100% |██████████████████████████████████████████████████| (2316/2316, 2072 it/s) [1s:0s]
$
```

search command で検索

```
$ vfs-index search  -q="2011_04" --index=../idx --column=name --table=test --data=./
{"id":130988433,"name":"2011_04_24-1.m4v"}
{"id":130988434,"name":"2011_04_24-2.mp4"}
{"id":130988435,"name":"2011_04_24-3.mp4"}
index merging done [==============================================================] 67507 / 67507
```


## goで使う

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

- [x] file list indexの書き出し
- [x] file list indexの読み込み
- [x] num column index の書き出し
- [x] num column index の読み込み
- [x] tri-gram clumn index の書き出し
- [x] 文字列検索の検索の実装
- [x] index merging のサポート
- [x] コメント、ライセンス追加
- [ ] csv のsupport
- [ ] msgpack のsupport
- [ ] dirty list のcontainer-list 化
- [ ] support lz4(?)