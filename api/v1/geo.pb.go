// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.6.1
// source: api/v1/geo.proto

package api_v1

import (
	proto "github.com/golang/protobuf/proto"
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

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type GeoPoint struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Latitude  float64 `protobuf:"fixed64,1,opt,name=latitude,proto3" json:"latitude,omitempty"`
	Longitude float64 `protobuf:"fixed64,2,opt,name=longitude,proto3" json:"longitude,omitempty"`
}

func (x *GeoPoint) Reset() {
	*x = GeoPoint{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_v1_geo_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GeoPoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GeoPoint) ProtoMessage() {}

func (x *GeoPoint) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_geo_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GeoPoint.ProtoReflect.Descriptor instead.
func (*GeoPoint) Descriptor() ([]byte, []int) {
	return file_api_v1_geo_proto_rawDescGZIP(), []int{0}
}

func (x *GeoPoint) GetLatitude() float64 {
	if x != nil {
		return x.Latitude
	}
	return 0
}

func (x *GeoPoint) GetLongitude() float64 {
	if x != nil {
		return x.Longitude
	}
	return 0
}

type PutRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Point *GeoPoint `protobuf:"bytes,1,opt,name=point,proto3" json:"point,omitempty"`
	Data  []byte    `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *PutRequest) Reset() {
	*x = PutRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_v1_geo_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutRequest) ProtoMessage() {}

func (x *PutRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_geo_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutRequest.ProtoReflect.Descriptor instead.
func (*PutRequest) Descriptor() ([]byte, []int) {
	return file_api_v1_geo_proto_rawDescGZIP(), []int{1}
}

func (x *PutRequest) GetPoint() *GeoPoint {
	if x != nil {
		return x.Point
	}
	return nil
}

func (x *PutRequest) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type GetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Point *GeoPoint `protobuf:"bytes,1,opt,name=point,proto3" json:"point,omitempty"`
}

func (x *GetRequest) Reset() {
	*x = GetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_v1_geo_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRequest) ProtoMessage() {}

func (x *GetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_geo_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRequest.ProtoReflect.Descriptor instead.
func (*GetRequest) Descriptor() ([]byte, []int) {
	return file_api_v1_geo_proto_rawDescGZIP(), []int{2}
}

func (x *GetRequest) GetPoint() *GeoPoint {
	if x != nil {
		return x.Point
	}
	return nil
}

type GetResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *GetResponse) Reset() {
	*x = GetResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_v1_geo_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResponse) ProtoMessage() {}

func (x *GetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_geo_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetResponse.ProtoReflect.Descriptor instead.
func (*GetResponse) Descriptor() ([]byte, []int) {
	return file_api_v1_geo_proto_rawDescGZIP(), []int{3}
}

func (x *GetResponse) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type PutResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
}

func (x *PutResponse) Reset() {
	*x = PutResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_v1_geo_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResponse) ProtoMessage() {}

func (x *PutResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_geo_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutResponse.ProtoReflect.Descriptor instead.
func (*PutResponse) Descriptor() ([]byte, []int) {
	return file_api_v1_geo_proto_rawDescGZIP(), []int{4}
}

func (x *PutResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

var File_api_v1_geo_proto protoreflect.FileDescriptor

var file_api_v1_geo_proto_rawDesc = []byte{
	0x0a, 0x10, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x67, 0x65, 0x6f, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x44, 0x0a, 0x08, 0x47, 0x65, 0x6f, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x1a,
	0x0a, 0x08, 0x6c, 0x61, 0x74, 0x69, 0x74, 0x75, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x01,
	0x52, 0x08, 0x6c, 0x61, 0x74, 0x69, 0x74, 0x75, 0x64, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x6c, 0x6f,
	0x6e, 0x67, 0x69, 0x74, 0x75, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x52, 0x09, 0x6c,
	0x6f, 0x6e, 0x67, 0x69, 0x74, 0x75, 0x64, 0x65, 0x22, 0x41, 0x0a, 0x0a, 0x50, 0x75, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1f, 0x0a, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x47, 0x65, 0x6f, 0x50, 0x6f, 0x69, 0x6e, 0x74,
	0x52, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x2d, 0x0a, 0x0a, 0x47,
	0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1f, 0x0a, 0x05, 0x70, 0x6f, 0x69,
	0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x47, 0x65, 0x6f, 0x50, 0x6f,
	0x69, 0x6e, 0x74, 0x52, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x22, 0x21, 0x0a, 0x0b, 0x47, 0x65,
	0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74,
	0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x22, 0x27, 0x0a,
	0x0b, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07,
	0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73,
	0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x32, 0x4d, 0x0a, 0x03, 0x47, 0x65, 0x6f, 0x12, 0x22, 0x0a,
	0x03, 0x50, 0x75, 0x74, 0x12, 0x0b, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x0c, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x12, 0x22, 0x0a, 0x03, 0x47, 0x65, 0x74, 0x12, 0x0b, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0c, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x1e, 0x5a, 0x1c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x6d, 0x6f, 0x68, 0x69, 0x74, 0x6b, 0x75, 0x6d, 0x61, 0x72, 0x2f, 0x61,
	0x70, 0x69, 0x5f, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_v1_geo_proto_rawDescOnce sync.Once
	file_api_v1_geo_proto_rawDescData = file_api_v1_geo_proto_rawDesc
)

func file_api_v1_geo_proto_rawDescGZIP() []byte {
	file_api_v1_geo_proto_rawDescOnce.Do(func() {
		file_api_v1_geo_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_v1_geo_proto_rawDescData)
	})
	return file_api_v1_geo_proto_rawDescData
}

var file_api_v1_geo_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_api_v1_geo_proto_goTypes = []interface{}{
	(*GeoPoint)(nil),    // 0: GeoPoint
	(*PutRequest)(nil),  // 1: PutRequest
	(*GetRequest)(nil),  // 2: GetRequest
	(*GetResponse)(nil), // 3: GetResponse
	(*PutResponse)(nil), // 4: PutResponse
}
var file_api_v1_geo_proto_depIdxs = []int32{
	0, // 0: PutRequest.point:type_name -> GeoPoint
	0, // 1: GetRequest.point:type_name -> GeoPoint
	1, // 2: Geo.Put:input_type -> PutRequest
	2, // 3: Geo.Get:input_type -> GetRequest
	4, // 4: Geo.Put:output_type -> PutResponse
	3, // 5: Geo.Get:output_type -> GetResponse
	4, // [4:6] is the sub-list for method output_type
	2, // [2:4] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_api_v1_geo_proto_init() }
func file_api_v1_geo_proto_init() {
	if File_api_v1_geo_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_v1_geo_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GeoPoint); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_v1_geo_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_v1_geo_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_v1_geo_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_v1_geo_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_v1_geo_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_api_v1_geo_proto_goTypes,
		DependencyIndexes: file_api_v1_geo_proto_depIdxs,
		MessageInfos:      file_api_v1_geo_proto_msgTypes,
	}.Build()
	File_api_v1_geo_proto = out.File
	file_api_v1_geo_proto_rawDesc = nil
	file_api_v1_geo_proto_goTypes = nil
	file_api_v1_geo_proto_depIdxs = nil
}
