// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

type IndexNumList struct { // genny
	*CommonNode
}

// IndexNum genny
func NewIndexNumList() *IndexNumList {

	list := emptyIndexNumList()
	list.NodeList = &base.NodeList{}
	list.CommonNode.Name = "[]IndexNum"

	(*base.List)(list.CommonNode).InitList()
	return list
}

func emptyIndexNumList() *IndexNumList {
	return &IndexNumList{CommonNode: &base.CommonNode{}}
}

func (node IndexNumList) At(i int) (result *IndexNum, e error) {
	result = &IndexNum{}
	result.CommonNode, e = (*base.List)(node.CommonNode).At(i)
	return
}

func (node IndexNumList) SetAt(i int, v *IndexNum) error {
	return (*base.List)(node.CommonNode).SetAt(i, v.CommonNode)
}

func (node IndexNumList) First() (result *IndexNum, e error) {
	return node.At(0)
}

func (node IndexNumList) Last() (result *IndexNum, e error) {
	return node.At(int(node.NodeList.ValueInfo.VLen) - 1)
}

func (node IndexNumList) Select(fn func(*IndexNum) bool) (result []*IndexNum) {
	result = make([]*IndexNum, 0, int(node.NodeList.ValueInfo.VLen))
	commons := (*base.List)(node.CommonNode).Select(func(cm *CommonNode) bool {
		return fn(&IndexNum{CommonNode: cm})
	})
	for _, cm := range commons {
		result = append(result, &IndexNum{CommonNode: cm})
	}
	return result
}

func (node IndexNumList) Find(fn func(*IndexNum) bool) *IndexNum {
	result := &IndexNum{}
	result.CommonNode = (*base.List)(node.CommonNode).Find(func(cm *CommonNode) bool {
		return fn(&IndexNum{CommonNode: cm})
	})
	return result
}

func (node IndexNumList) All() []*IndexNum {
	return node.Select(func(*IndexNum) bool { return true })
}

func (node IndexNumList) Count() int {
	return int(node.NodeList.ValueInfo.VLen)
}

func (node IndexNumList) SwapAt(i, j int) error {
	return (*List)(node.CommonNode).SwapAt(i, j)
}

func (node IndexNumList) SortBy(less func(i, j int) bool) error {
	return (*List)(node.CommonNode).SortBy(less)
}

// Search ... binary search
func (node IndexNumList) Search(fn func(*IndexNum) bool) *IndexNum {
	result := &IndexNum{}

	i := (*base.List)(node.CommonNode).SearchIndex(int((*base.List)(node.CommonNode).VLen()), func(cm *CommonNode) bool {
		return fn(&IndexNum{CommonNode: cm})
	})
	if i < int((*base.List)(node.CommonNode).VLen()) {
		result, _ = node.At(i)
	}

	return result
}
