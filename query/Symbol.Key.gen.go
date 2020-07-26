// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

/*
genny must be called per Field
*/

var (
	Symbol_Key_0 int = base.AtoiNoErr(Atoi("0"))
	Symbol_Key   int = Symbol_Key_0
)

// (field inedx, field type) -> Symbol_IdxToType
var DUMMY_Symbol_Key bool = SetSymbolFields("Symbol", "Key", "[][]byte", Symbol_Key_0)

func (node Symbol) Key() (result *CommonNodeList) {
	result = emptyCommonNodeList()
	common := node.FieldAt(Symbol_Key_0)
	if common.Node == nil {
		result = NewCommonNodeList()
		node.SetFieldAt(Symbol_Key_0, result.SelfAsCommonNode())
		common = node.FieldAt(Symbol_Key_0)
	}

	result.Name = common.Name
	result.NodeList = common.NodeList
	result.IdxToType = common.IdxToType
	result.IdxToTypeGroup = common.IdxToTypeGroup

	return
}

func (node Symbol) SetKey(v *base.CommonNode) error {

	return node.CommonNode.SetFieldAt(0, v)
}
