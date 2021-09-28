// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

type InvertedMapNumList struct { // genny
	*CommonNode
}

// InvertedMapNum genny
func NewInvertedMapNumList() *InvertedMapNumList {

	list := emptyInvertedMapNumList()
	list.NodeList = &base.NodeList{}
	list.CommonNode.Name = "[]InvertedMapNum"

	(*base.List)(list.CommonNode).InitList()
	return list
}

func emptyInvertedMapNumList() *InvertedMapNumList {
	return &InvertedMapNumList{CommonNode: &base.CommonNode{}}
}

func (node InvertedMapNumList) At(i int) (result *InvertedMapNum, e error) {
	result = &InvertedMapNum{}
	result.CommonNode, e = (*base.List)(node.CommonNode).At(i)
	return
}

func (node InvertedMapNumList) AtWihoutError(i int) (result *InvertedMapNum) {
	result, e := node.At(i)
	if e != nil {
		result = nil
	}
	return
}

func (node InvertedMapNumList) SetAt(i int, v *InvertedMapNum) error {
	return (*base.List)(node.CommonNode).SetAt(i, v.CommonNode)
}

func (node InvertedMapNumList) First() (result *InvertedMapNum, e error) {
	return node.At(0)
}

func (node InvertedMapNumList) Last() (result *InvertedMapNum, e error) {
	return node.At(int(node.NodeList.ValueInfo.VLen) - 1)
}

func (node InvertedMapNumList) Select(fn func(*InvertedMapNum) bool) (result []*InvertedMapNum) {
	result = make([]*InvertedMapNum, 0, int(node.NodeList.ValueInfo.VLen))
	commons := (*base.List)(node.CommonNode).Select(func(cm *CommonNode) bool {
		return fn(&InvertedMapNum{CommonNode: cm})
	})
	for _, cm := range commons {
		result = append(result, &InvertedMapNum{CommonNode: cm})
	}
	return result
}

func (node InvertedMapNumList) Find(fn func(*InvertedMapNum) bool) *InvertedMapNum {
	result := &InvertedMapNum{}
	result.CommonNode = (*base.List)(node.CommonNode).Find(func(cm *CommonNode) bool {
		return fn(&InvertedMapNum{CommonNode: cm})
	})
	return result
}

func (node InvertedMapNumList) All() []*InvertedMapNum {
	return node.Select(func(*InvertedMapNum) bool { return true })
}

func (node InvertedMapNumList) Count() int {
	return int(node.NodeList.ValueInfo.VLen)
}

func (node InvertedMapNumList) SwapAt(i, j int) error {
	return (*List)(node.CommonNode).SwapAt(i, j)
}

func (node InvertedMapNumList) SortBy(less func(i, j int) bool) error {
	return (*List)(node.CommonNode).SortBy(less)
}

// Search ... binary search
func (node InvertedMapNumList) Search(fn func(*InvertedMapNum) bool) *InvertedMapNum {
	result := &InvertedMapNum{}

	i := (*base.List)(node.CommonNode).SearchIndex(int((*base.List)(node.CommonNode).VLen()), func(cm *CommonNode) bool {
		return fn(&InvertedMapNum{CommonNode: cm})
	})
	if i < int((*base.List)(node.CommonNode).VLen()) {
		result, _ = node.At(i)
	}

	return result
}

func (node InvertedMapNumList) SearchIndex(fn func(*InvertedMapNum) bool) int {

	i := (*base.List)(node.CommonNode).SearchIndex(int((*base.List)(node.CommonNode).VLen()), func(cm *CommonNode) bool {
		return fn(&InvertedMapNum{CommonNode: cm})
	})
	if i < int((*base.List)(node.CommonNode).VLen()) {
		return i
	}

	return -1
}
