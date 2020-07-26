// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

/*
genny must be called per Field
*/

var (
	Record_OffsetOfValue_3 int = base.AtoiNoErr(Atoi("3"))
	Record_OffsetOfValue   int = Record_OffsetOfValue_3
)

// (field inedx, field type) -> Record_IdxToType
var DUMMY_Record_OffsetOfValue bool = SetRecordFields("Record", "OffsetOfValue", "Int32", Record_OffsetOfValue_3)

func (node Record) OffsetOfValue() (result *CommonNode) {
	result = emptyCommonNode()
	common := node.FieldAt(Record_OffsetOfValue_3)
	if common.Node == nil {
		result = NewCommonNode()
		node.SetFieldAt(Record_OffsetOfValue_3, result.SelfAsCommonNode())
		common = node.FieldAt(Record_OffsetOfValue_3)
	}

	result.Name = common.Name
	result.NodeList = common.NodeList
	result.IdxToType = common.IdxToType
	result.IdxToTypeGroup = common.IdxToTypeGroup

	return
}

func (node Record) SetOffsetOfValue(v *base.CommonNode) error {

	return node.CommonNode.SetFieldAt(3, v)
}
