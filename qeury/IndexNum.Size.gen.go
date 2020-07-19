// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

/*
genny must be called per Field
*/

var (
	IndexNum_Size_0 int = base.AtoiNoErr(Atoi("0"))
	IndexNum_Size   int = IndexNum_Size_0
)

// (field inedx, field type) -> IndexNum_IdxToType
var DUMMY_IndexNum_Size bool = SetIndexNumFields("IndexNum", "Size", "Int32", IndexNum_Size_0)

func (node IndexNum) Size() (result *CommonNode) {
	result = emptyCommonNode()
	common := node.FieldAt(IndexNum_Size_0)

	result.Name = common.Name
	result.NodeList = common.NodeList
	result.IdxToType = common.IdxToType
	result.IdxToTypeGroup = common.IdxToTypeGroup

	return
}

func (node IndexNum) SetSize(v *base.CommonNode) error {

	return node.CommonNode.SetFieldAt(0, v)
}