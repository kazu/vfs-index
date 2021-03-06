// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

/*
genny must be called per Field
*/

var (
	Record_ValueSize_4 int = base.AtoiNoErr(Atoi("4"))
	Record_ValueSize   int = Record_ValueSize_4
)

// (field inedx, field type) -> Record_IdxToType
var DUMMY_Record_ValueSize bool = SetRecordFields("Record", "ValueSize", "Int32", Record_ValueSize_4)

func (node Record) ValueSize() (result *CommonNode) {
	result = emptyCommonNode()
	common := node.FieldAt(Record_ValueSize_4)
	if common.Node == nil {
		result = NewCommonNode()
		node.SetFieldAt(Record_ValueSize_4, result.SelfAsCommonNode())
		common = node.FieldAt(Record_ValueSize_4)
	}

	result.Name = common.Name
	result.NodeList = common.NodeList
	result.IdxToType = common.IdxToType
	result.IdxToTypeGroup = common.IdxToTypeGroup

	return
}

//func (node Record) SetValueSize(v *base.CommonNode) error {
func (node Record) SetValueSize(v *CommonNode) error {

	return node.CommonNode.SetFieldAt(4, v.SelfAsCommonNode())
}
