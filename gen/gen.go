package gen

//go:generate flatc --go -o ../ ../spec/index.fbs
//go:generate go run github.com/kazu/fbshelper/cmd/fbs-query --fbs=../spec/index.fbs --out=../qeury
