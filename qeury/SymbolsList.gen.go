// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

type SymbolsList struct { // genny
	*CommonNode
}

// Symbols genny
func NewSymbolsList() *SymbolsList {

	return emptySymbolsList()
}

func emptySymbolsList() *SymbolsList {
	return &SymbolsList{CommonNode: &base.CommonNode{}}
}

func (node SymbolsList) At(i int) (result *Symbols, e error) {
	result = &Symbols{}
	result.CommonNode, e = node.CommonNode.At(i)
	return
}

func (node SymbolsList) First() (result *Symbols, e error) {
	return node.At(0)
}

func (node SymbolsList) Last() (result *Symbols, e error) {
	return node.At(int(node.NodeList.ValueInfo.VLen) - 1)
}

func (node SymbolsList) Select(fn func(*Symbols) bool) (result []*Symbols) {
	result = make([]*Symbols, 0, int(node.NodeList.ValueInfo.VLen))
	commons := node.CommonNode.Select(func(cm *CommonNode) bool {
		return fn(&Symbols{CommonNode: cm})
	})
	for _, cm := range commons {
		result = append(result, &Symbols{CommonNode: cm})
	}
	return result
}

func (node SymbolsList) Find(fn func(*Symbols) bool) *Symbols {
	result := &Symbols{}
	result.CommonNode = node.CommonNode.Find(func(cm *CommonNode) bool {
		return fn(&Symbols{CommonNode: cm})
	})
	return result
}

func (node SymbolsList) All() []*Symbols {
	return node.Select(func(*Symbols) bool { return true })
}

func (node SymbolsList) Count() int {
	return int(node.NodeList.ValueInfo.VLen)
}
