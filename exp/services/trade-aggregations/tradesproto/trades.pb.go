// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v5.28.2
// source: exp/services/trade-aggregations/trades.proto

package tradesproto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Fraction struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The portion of the denominator in the faction, e.g. 2 in 2/3.
	Numerator int64 `protobuf:"varint,1,opt,name=numerator,proto3" json:"numerator,omitempty"`
	// The value by which the numerator is divided, e.g. 3 in 2/3. Must be
	// positive.
	Denominator int64 `protobuf:"varint,2,opt,name=denominator,proto3" json:"denominator,omitempty"`
}

func (x *Fraction) Reset() {
	*x = Fraction{}
	mi := &file_exp_services_trade_aggregations_trades_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Fraction) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Fraction) ProtoMessage() {}

func (x *Fraction) ProtoReflect() protoreflect.Message {
	mi := &file_exp_services_trade_aggregations_trades_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Fraction.ProtoReflect.Descriptor instead.
func (*Fraction) Descriptor() ([]byte, []int) {
	return file_exp_services_trade_aggregations_trades_proto_rawDescGZIP(), []int{0}
}

func (x *Fraction) GetNumerator() int64 {
	if x != nil {
		return x.Numerator
	}
	return 0
}

func (x *Fraction) GetDenominator() int64 {
	if x != nil {
		return x.Denominator
	}
	return 0
}

type Aggregation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp     int64     `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Count         int64     `protobuf:"varint,2,opt,name=count,proto3" json:"count,omitempty"`
	BaseVolume    []byte    `protobuf:"bytes,3,opt,name=base_volume,json=baseVolume,proto3" json:"base_volume,omitempty"`
	CounterVolume []byte    `protobuf:"bytes,4,opt,name=counter_volume,json=counterVolume,proto3" json:"counter_volume,omitempty"`
	High          *Fraction `protobuf:"bytes,5,opt,name=high,proto3" json:"high,omitempty"`
	Low           *Fraction `protobuf:"bytes,6,opt,name=low,proto3" json:"low,omitempty"`
	Open          *Fraction `protobuf:"bytes,7,opt,name=open,proto3" json:"open,omitempty"`
	Close         *Fraction `protobuf:"bytes,8,opt,name=close,proto3" json:"close,omitempty"`
}

func (x *Aggregation) Reset() {
	*x = Aggregation{}
	mi := &file_exp_services_trade_aggregations_trades_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Aggregation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Aggregation) ProtoMessage() {}

func (x *Aggregation) ProtoReflect() protoreflect.Message {
	mi := &file_exp_services_trade_aggregations_trades_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Aggregation.ProtoReflect.Descriptor instead.
func (*Aggregation) Descriptor() ([]byte, []int) {
	return file_exp_services_trade_aggregations_trades_proto_rawDescGZIP(), []int{1}
}

func (x *Aggregation) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Aggregation) GetCount() int64 {
	if x != nil {
		return x.Count
	}
	return 0
}

func (x *Aggregation) GetBaseVolume() []byte {
	if x != nil {
		return x.BaseVolume
	}
	return nil
}

func (x *Aggregation) GetCounterVolume() []byte {
	if x != nil {
		return x.CounterVolume
	}
	return nil
}

func (x *Aggregation) GetHigh() *Fraction {
	if x != nil {
		return x.High
	}
	return nil
}

func (x *Aggregation) GetLow() *Fraction {
	if x != nil {
		return x.Low
	}
	return nil
}

func (x *Aggregation) GetOpen() *Fraction {
	if x != nil {
		return x.Open
	}
	return nil
}

func (x *Aggregation) GetClose() *Fraction {
	if x != nil {
		return x.Close
	}
	return nil
}

type Bucket struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Aggregations []*Aggregation `protobuf:"bytes,1,rep,name=aggregations,proto3" json:"aggregations,omitempty"`
}

func (x *Bucket) Reset() {
	*x = Bucket{}
	mi := &file_exp_services_trade_aggregations_trades_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Bucket) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Bucket) ProtoMessage() {}

func (x *Bucket) ProtoReflect() protoreflect.Message {
	mi := &file_exp_services_trade_aggregations_trades_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Bucket.ProtoReflect.Descriptor instead.
func (*Bucket) Descriptor() ([]byte, []int) {
	return file_exp_services_trade_aggregations_trades_proto_rawDescGZIP(), []int{2}
}

func (x *Bucket) GetAggregations() []*Aggregation {
	if x != nil {
		return x.Aggregations
	}
	return nil
}

var File_exp_services_trade_aggregations_trades_proto protoreflect.FileDescriptor

var file_exp_services_trade_aggregations_trades_proto_rawDesc = []byte{
	0x0a, 0x2c, 0x65, 0x78, 0x70, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x74,
	0x72, 0x61, 0x64, 0x65, 0x2d, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x2f, 0x74, 0x72, 0x61, 0x64, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x74, 0x72, 0x61, 0x64, 0x65, 0x73, 0x22, 0x4a, 0x0a, 0x08, 0x46, 0x72, 0x61, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x75, 0x6d, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x6e, 0x75, 0x6d, 0x65, 0x72, 0x61, 0x74, 0x6f, 0x72,
	0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x6e, 0x6f, 0x6d, 0x69, 0x6e, 0x61, 0x74, 0x6f, 0x72, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x64, 0x65, 0x6e, 0x6f, 0x6d, 0x69, 0x6e, 0x61, 0x74,
	0x6f, 0x72, 0x22, 0xa1, 0x02, 0x0a, 0x0b, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x12, 0x14, 0x0a, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x1f, 0x0a, 0x0b, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x76,
	0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0a, 0x62, 0x61, 0x73,
	0x65, 0x56, 0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x63, 0x6f, 0x75, 0x6e, 0x74,
	0x65, 0x72, 0x5f, 0x76, 0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x0d, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x56, 0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x12, 0x24,
	0x0a, 0x04, 0x68, 0x69, 0x67, 0x68, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x74,
	0x72, 0x61, 0x64, 0x65, 0x73, 0x2e, 0x46, 0x72, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x04,
	0x68, 0x69, 0x67, 0x68, 0x12, 0x22, 0x0a, 0x03, 0x6c, 0x6f, 0x77, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x10, 0x2e, 0x74, 0x72, 0x61, 0x64, 0x65, 0x73, 0x2e, 0x46, 0x72, 0x61, 0x63, 0x74,
	0x69, 0x6f, 0x6e, 0x52, 0x03, 0x6c, 0x6f, 0x77, 0x12, 0x24, 0x0a, 0x04, 0x6f, 0x70, 0x65, 0x6e,
	0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x74, 0x72, 0x61, 0x64, 0x65, 0x73, 0x2e,
	0x46, 0x72, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x04, 0x6f, 0x70, 0x65, 0x6e, 0x12, 0x26,
	0x0a, 0x05, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e,
	0x74, 0x72, 0x61, 0x64, 0x65, 0x73, 0x2e, 0x46, 0x72, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x05, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x22, 0x41, 0x0a, 0x06, 0x42, 0x75, 0x63, 0x6b, 0x65, 0x74,
	0x12, 0x37, 0x0a, 0x0c, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x74, 0x72, 0x61, 0x64, 0x65, 0x73, 0x2e,
	0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0c, 0x61, 0x67, 0x67,
	0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x42, 0x2d, 0x5a, 0x2b, 0x65, 0x78, 0x70,
	0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x74, 0x72, 0x61, 0x64, 0x65, 0x2d,
	0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x74, 0x72, 0x61,
	0x64, 0x65, 0x73, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_exp_services_trade_aggregations_trades_proto_rawDescOnce sync.Once
	file_exp_services_trade_aggregations_trades_proto_rawDescData = file_exp_services_trade_aggregations_trades_proto_rawDesc
)

func file_exp_services_trade_aggregations_trades_proto_rawDescGZIP() []byte {
	file_exp_services_trade_aggregations_trades_proto_rawDescOnce.Do(func() {
		file_exp_services_trade_aggregations_trades_proto_rawDescData = protoimpl.X.CompressGZIP(file_exp_services_trade_aggregations_trades_proto_rawDescData)
	})
	return file_exp_services_trade_aggregations_trades_proto_rawDescData
}

var file_exp_services_trade_aggregations_trades_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_exp_services_trade_aggregations_trades_proto_goTypes = []any{
	(*Fraction)(nil),    // 0: trades.Fraction
	(*Aggregation)(nil), // 1: trades.Aggregation
	(*Bucket)(nil),      // 2: trades.Bucket
}
var file_exp_services_trade_aggregations_trades_proto_depIdxs = []int32{
	0, // 0: trades.Aggregation.high:type_name -> trades.Fraction
	0, // 1: trades.Aggregation.low:type_name -> trades.Fraction
	0, // 2: trades.Aggregation.open:type_name -> trades.Fraction
	0, // 3: trades.Aggregation.close:type_name -> trades.Fraction
	1, // 4: trades.Bucket.aggregations:type_name -> trades.Aggregation
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_exp_services_trade_aggregations_trades_proto_init() }
func file_exp_services_trade_aggregations_trades_proto_init() {
	if File_exp_services_trade_aggregations_trades_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_exp_services_trade_aggregations_trades_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_exp_services_trade_aggregations_trades_proto_goTypes,
		DependencyIndexes: file_exp_services_trade_aggregations_trades_proto_depIdxs,
		MessageInfos:      file_exp_services_trade_aggregations_trades_proto_msgTypes,
	}.Build()
	File_exp_services_trade_aggregations_trades_proto = out.File
	file_exp_services_trade_aggregations_trades_proto_rawDesc = nil
	file_exp_services_trade_aggregations_trades_proto_goTypes = nil
	file_exp_services_trade_aggregations_trades_proto_depIdxs = nil
}
