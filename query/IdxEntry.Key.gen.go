// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

/*
genny must be called per Field
*/

var (
	IdxEntry_Key_0 int = base.AtoiNoErr(Atoi("0"))
	IdxEntry_Key   int = IdxEntry_Key_0
)

// (field inedx, field type) -> IdxEntry_IdxToType
var DUMMY_IdxEntry_Key bool = SetIdxEntryFields("IdxEntry", "Key", "[]byte", IdxEntry_Key_0)

func (node IdxEntry) Key() (result *CommonNode) {
	result = emptyCommonNode()
	common := node.FieldAt(IdxEntry_Key_0)
	if common.Node == nil {
		result = NewCommonNode()
		node.SetFieldAt(IdxEntry_Key_0, result.SelfAsCommonNode())
		common = node.FieldAt(IdxEntry_Key_0)
	}

	result.Name = common.Name
	result.NodeList = common.NodeList
	result.IdxToType = common.IdxToType
	result.IdxToTypeGroup = common.IdxToTypeGroup

	return
}

func (node IdxEntry) SetKey(v *base.CommonNode) error {

	return node.CommonNode.SetFieldAt(0, v)
}