package gen

import (
	_ "github.com/cheekybits/genny"
)

//go:generate flatc --go -o ../ ../spec/index.fbs
//go:generate go run github.com/kazu/fbshelper/cmd/fbs-query -v --fbs=../spec/index.fbs --out=../qeury
