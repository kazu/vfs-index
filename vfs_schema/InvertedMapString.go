// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package vfs_schema

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type InvertedMapString struct {
	_tab flatbuffers.Table
}

func GetRootAsInvertedMapString(buf []byte, offset flatbuffers.UOffsetT) *InvertedMapString {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &InvertedMapString{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *InvertedMapString) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *InvertedMapString) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *InvertedMapString) Key() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *InvertedMapString) MutateKey(n int32) bool {
	return rcv._tab.MutateInt32Slot(4, n)
}

func (rcv *InvertedMapString) Value(obj *Record) *Record {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Record)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func InvertedMapStringStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func InvertedMapStringAddKey(builder *flatbuffers.Builder, key int32) {
	builder.PrependInt32Slot(0, key, 0)
}
func InvertedMapStringAddValue(builder *flatbuffers.Builder, value flatbuffers.UOffsetT) {
	builder.PrependStructSlot(1, flatbuffers.UOffsetT(value), 0)
}
func InvertedMapStringEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
