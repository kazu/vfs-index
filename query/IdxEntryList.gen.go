// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

type IdxEntryList struct { // genny
	*CommonNode
}

// IdxEntry genny
func NewIdxEntryList() *IdxEntryList {

	list := emptyIdxEntryList()
	list.NodeList = &base.NodeList{}
	list.CommonNode.Name = "[]IdxEntry"

	(*base.List)(list.CommonNode).InitList()
	return list
}

func emptyIdxEntryList() *IdxEntryList {
	return &IdxEntryList{CommonNode: &base.CommonNode{}}
}

func (node IdxEntryList) At(i int) (result *IdxEntry, e error) {
	result = &IdxEntry{}
	result.CommonNode, e = (*base.List)(node.CommonNode).At(i)
	return
}

func (node IdxEntryList) SetAt(i int, v *IdxEntry) error {
	return (*base.List)(node.CommonNode).SetAt(i, v.CommonNode)
}

func (node IdxEntryList) First() (result *IdxEntry, e error) {
	return node.At(0)
}

func (node IdxEntryList) Last() (result *IdxEntry, e error) {
	return node.At(int(node.NodeList.ValueInfo.VLen) - 1)
}

func (node IdxEntryList) Select(fn func(*IdxEntry) bool) (result []*IdxEntry) {
	result = make([]*IdxEntry, 0, int(node.NodeList.ValueInfo.VLen))
	commons := (*base.List)(node.CommonNode).Select(func(cm *CommonNode) bool {
		return fn(&IdxEntry{CommonNode: cm})
	})
	for _, cm := range commons {
		result = append(result, &IdxEntry{CommonNode: cm})
	}
	return result
}

func (node IdxEntryList) Find(fn func(*IdxEntry) bool) *IdxEntry {
	result := &IdxEntry{}
	result.CommonNode = (*base.List)(node.CommonNode).Find(func(cm *CommonNode) bool {
		return fn(&IdxEntry{CommonNode: cm})
	})
	return result
}

func (node IdxEntryList) All() []*IdxEntry {
	return node.Select(func(*IdxEntry) bool { return true })
}

func (node IdxEntryList) Count() int {
	return int(node.NodeList.ValueInfo.VLen)
}

func (node IdxEntryList) SwapAt(i, j int) error {
	return (*List)(node.CommonNode).SwapAt(i, j)
}

func (node IdxEntryList) SortBy(less func(i, j int) bool) error {
	return (*List)(node.CommonNode).SortBy(less)
}

// Search ... binary search
func (node IdxEntryList) Search(fn func(*IdxEntry) bool) *IdxEntry {
	result := &IdxEntry{}

	i := (*base.List)(node.CommonNode).SearchIndex(int((*base.List)(node.CommonNode).VLen()), func(cm *CommonNode) bool {
		return fn(&IdxEntry{CommonNode: cm})
	})
	if i < int((*base.List)(node.CommonNode).VLen()) {
		result, _ = node.At(i)
	}

	return result
}
