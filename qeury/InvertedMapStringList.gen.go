// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

type InvertedMapStringList struct { // genny
	*CommonNode
}

// InvertedMapString genny
func NewInvertedMapStringList() *InvertedMapStringList {

	return emptyInvertedMapStringList()
}

func emptyInvertedMapStringList() *InvertedMapStringList {
	return &InvertedMapStringList{CommonNode: &base.CommonNode{}}
}

func (node InvertedMapStringList) At(i int) (result *InvertedMapString, e error) {
	result = &InvertedMapString{}
	result.CommonNode, e = node.CommonNode.At(i)
	return
}

func (node InvertedMapStringList) First() (result *InvertedMapString, e error) {
	return node.At(0)
}

func (node InvertedMapStringList) Last() (result *InvertedMapString, e error) {
	return node.At(int(node.NodeList.ValueInfo.VLen) - 1)
}

func (node InvertedMapStringList) Select(fn func(*InvertedMapString) bool) (result []*InvertedMapString) {
	result = make([]*InvertedMapString, 0, int(node.NodeList.ValueInfo.VLen))
	commons := node.CommonNode.Select(func(cm *CommonNode) bool {
		return fn(&InvertedMapString{CommonNode: cm})
	})
	for _, cm := range commons {
		result = append(result, &InvertedMapString{CommonNode: cm})
	}
	return result
}

func (node InvertedMapStringList) Find(fn func(*InvertedMapString) bool) *InvertedMapString {
	result := &InvertedMapString{}
	result.CommonNode = node.CommonNode.Find(func(cm *CommonNode) bool {
		return fn(&InvertedMapString{CommonNode: cm})
	})
	return result
}

func (node InvertedMapStringList) All() []*InvertedMapString {
	return node.Select(func(*InvertedMapString) bool { return true })
}

func (node InvertedMapStringList) Count() int {
	return int(node.NodeList.ValueInfo.VLen)
}
