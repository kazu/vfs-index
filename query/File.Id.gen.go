// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package query

import "github.com/kazu/fbshelper/query/base"

/*
genny must be called per Field
*/

var (
	File_Id_0 int = base.AtoiNoErr(Atoi("0"))
	File_Id   int = File_Id_0
)

// (field inedx, field type) -> File_IdxToType
var DUMMY_File_Id bool = SetFileFields("File", "Id", "Uint64", File_Id_0)

func (node File) Id() (result *CommonNode) {
	result = emptyCommonNode()
	common := node.FieldAt(File_Id_0)
	if common.Node == nil {
		result = NewCommonNode()
		node.SetFieldAt(File_Id_0, result.SelfAsCommonNode())
		common = node.FieldAt(File_Id_0)
	}

	result.Name = common.Name
	result.NodeList = common.NodeList
	result.IdxToType = common.IdxToType
	result.IdxToTypeGroup = common.IdxToTypeGroup

	return
}

func (node File) SetId(v *base.CommonNode) error {

	return node.CommonNode.SetFieldAt(0, v)
}
