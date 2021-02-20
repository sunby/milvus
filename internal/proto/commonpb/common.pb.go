// Code generated by protoc-gen-go. DO NOT EDIT.
// source: common.proto

package commonpb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ErrorCode int32

const (
	ErrorCode_SUCCESS                 ErrorCode = 0
	ErrorCode_UNEXPECTED_ERROR        ErrorCode = 1
	ErrorCode_CONNECT_FAILED          ErrorCode = 2
	ErrorCode_PERMISSION_DENIED       ErrorCode = 3
	ErrorCode_COLLECTION_NOT_EXISTS   ErrorCode = 4
	ErrorCode_ILLEGAL_ARGUMENT        ErrorCode = 5
	ErrorCode_ILLEGAL_DIMENSION       ErrorCode = 7
	ErrorCode_ILLEGAL_INDEX_TYPE      ErrorCode = 8
	ErrorCode_ILLEGAL_COLLECTION_NAME ErrorCode = 9
	ErrorCode_ILLEGAL_TOPK            ErrorCode = 10
	ErrorCode_ILLEGAL_ROWRECORD       ErrorCode = 11
	ErrorCode_ILLEGAL_VECTOR_ID       ErrorCode = 12
	ErrorCode_ILLEGAL_SEARCH_RESULT   ErrorCode = 13
	ErrorCode_FILE_NOT_FOUND          ErrorCode = 14
	ErrorCode_META_FAILED             ErrorCode = 15
	ErrorCode_CACHE_FAILED            ErrorCode = 16
	ErrorCode_CANNOT_CREATE_FOLDER    ErrorCode = 17
	ErrorCode_CANNOT_CREATE_FILE      ErrorCode = 18
	ErrorCode_CANNOT_DELETE_FOLDER    ErrorCode = 19
	ErrorCode_CANNOT_DELETE_FILE      ErrorCode = 20
	ErrorCode_BUILD_INDEX_ERROR       ErrorCode = 21
	ErrorCode_ILLEGAL_NLIST           ErrorCode = 22
	ErrorCode_ILLEGAL_METRIC_TYPE     ErrorCode = 23
	ErrorCode_OUT_OF_MEMORY           ErrorCode = 24
	// internal error code.
	ErrorCode_DD_REQUEST_RACE ErrorCode = 1000
)

var ErrorCode_name = map[int32]string{
	0:    "SUCCESS",
	1:    "UNEXPECTED_ERROR",
	2:    "CONNECT_FAILED",
	3:    "PERMISSION_DENIED",
	4:    "COLLECTION_NOT_EXISTS",
	5:    "ILLEGAL_ARGUMENT",
	7:    "ILLEGAL_DIMENSION",
	8:    "ILLEGAL_INDEX_TYPE",
	9:    "ILLEGAL_COLLECTION_NAME",
	10:   "ILLEGAL_TOPK",
	11:   "ILLEGAL_ROWRECORD",
	12:   "ILLEGAL_VECTOR_ID",
	13:   "ILLEGAL_SEARCH_RESULT",
	14:   "FILE_NOT_FOUND",
	15:   "META_FAILED",
	16:   "CACHE_FAILED",
	17:   "CANNOT_CREATE_FOLDER",
	18:   "CANNOT_CREATE_FILE",
	19:   "CANNOT_DELETE_FOLDER",
	20:   "CANNOT_DELETE_FILE",
	21:   "BUILD_INDEX_ERROR",
	22:   "ILLEGAL_NLIST",
	23:   "ILLEGAL_METRIC_TYPE",
	24:   "OUT_OF_MEMORY",
	1000: "DD_REQUEST_RACE",
}

var ErrorCode_value = map[string]int32{
	"SUCCESS":                 0,
	"UNEXPECTED_ERROR":        1,
	"CONNECT_FAILED":          2,
	"PERMISSION_DENIED":       3,
	"COLLECTION_NOT_EXISTS":   4,
	"ILLEGAL_ARGUMENT":        5,
	"ILLEGAL_DIMENSION":       7,
	"ILLEGAL_INDEX_TYPE":      8,
	"ILLEGAL_COLLECTION_NAME": 9,
	"ILLEGAL_TOPK":            10,
	"ILLEGAL_ROWRECORD":       11,
	"ILLEGAL_VECTOR_ID":       12,
	"ILLEGAL_SEARCH_RESULT":   13,
	"FILE_NOT_FOUND":          14,
	"META_FAILED":             15,
	"CACHE_FAILED":            16,
	"CANNOT_CREATE_FOLDER":    17,
	"CANNOT_CREATE_FILE":      18,
	"CANNOT_DELETE_FOLDER":    19,
	"CANNOT_DELETE_FILE":      20,
	"BUILD_INDEX_ERROR":       21,
	"ILLEGAL_NLIST":           22,
	"ILLEGAL_METRIC_TYPE":     23,
	"OUT_OF_MEMORY":           24,
	"DD_REQUEST_RACE":         1000,
}

func (x ErrorCode) String() string {
	return proto.EnumName(ErrorCode_name, int32(x))
}

func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{0}
}

type IndexState int32

const (
	IndexState_NONE       IndexState = 0
	IndexState_UNISSUED   IndexState = 1
	IndexState_INPROGRESS IndexState = 2
	IndexState_FINISHED   IndexState = 3
	IndexState_FAILED     IndexState = 4
	IndexState_DELETED    IndexState = 5
)

var IndexState_name = map[int32]string{
	0: "NONE",
	1: "UNISSUED",
	2: "INPROGRESS",
	3: "FINISHED",
	4: "FAILED",
	5: "DELETED",
}

var IndexState_value = map[string]int32{
	"NONE":       0,
	"UNISSUED":   1,
	"INPROGRESS": 2,
	"FINISHED":   3,
	"FAILED":     4,
	"DELETED":    5,
}

func (x IndexState) String() string {
	return proto.EnumName(IndexState_name, int32(x))
}

func (IndexState) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{1}
}

type SegmentState int32

const (
	SegmentState_SegmentNone     SegmentState = 0
	SegmentState_SegmentNotExist SegmentState = 1
	SegmentState_SegmentGrowing  SegmentState = 2
	SegmentState_SegmentSealed   SegmentState = 3
	SegmentState_SegmentFlushed  SegmentState = 4
)

var SegmentState_name = map[int32]string{
	0: "SegmentNone",
	1: "SegmentNotExist",
	2: "SegmentGrowing",
	3: "SegmentSealed",
	4: "SegmentFlushed",
}

var SegmentState_value = map[string]int32{
	"SegmentNone":     0,
	"SegmentNotExist": 1,
	"SegmentGrowing":  2,
	"SegmentSealed":   3,
	"SegmentFlushed":  4,
}

func (x SegmentState) String() string {
	return proto.EnumName(SegmentState_name, int32(x))
}

func (SegmentState) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{2}
}

type MsgType int32

const (
	MsgType_kNone MsgType = 0
	// Definition Requests: collection
	MsgType_kCreateCollection   MsgType = 100
	MsgType_kDropCollection     MsgType = 101
	MsgType_kHasCollection      MsgType = 102
	MsgType_kDescribeCollection MsgType = 103
	MsgType_kShowCollections    MsgType = 104
	MsgType_kGetSysConfigs      MsgType = 105
	MsgType_kLoadCollection     MsgType = 106
	MsgType_kReleaseCollection  MsgType = 107
	// Definition Requests: partition
	MsgType_kCreatePartition   MsgType = 200
	MsgType_kDropPartition     MsgType = 201
	MsgType_kHasPartition      MsgType = 202
	MsgType_kDescribePartition MsgType = 203
	MsgType_kShowPartitions    MsgType = 204
	MsgType_kLoadPartition     MsgType = 205
	MsgType_kReleasePartition  MsgType = 206
	// Define Requests: segment
	MsgType_kShowSegment     MsgType = 250
	MsgType_kDescribeSegment MsgType = 251
	// Definition Requests: Index
	MsgType_kCreateIndex   MsgType = 300
	MsgType_kDescribeIndex MsgType = 301
	MsgType_kDropIndex     MsgType = 302
	// Manipulation Requests
	MsgType_kInsert MsgType = 400
	MsgType_kDelete MsgType = 401
	MsgType_kFlush  MsgType = 402
	// Query
	MsgType_kSearch                  MsgType = 500
	MsgType_kSearchResult            MsgType = 501
	MsgType_kGetIndexState           MsgType = 502
	MsgType_kGetCollectionStatistics MsgType = 503
	MsgType_kGetPartitionStatistics  MsgType = 504
	// Data Service
	MsgType_kSegmentInfo MsgType = 600
	// System Control
	MsgType_kTimeTick          MsgType = 1200
	MsgType_kQueryNodeStats    MsgType = 1201
	MsgType_kLoadIndex         MsgType = 1202
	MsgType_kRequestID         MsgType = 1203
	MsgType_kRequestTSO        MsgType = 1204
	MsgType_kAllocateSegment   MsgType = 1205
	MsgType_kSegmentStatistics MsgType = 1206
	MsgType_kSegmentFlushDone  MsgType = 1207
)

var MsgType_name = map[int32]string{
	0:    "kNone",
	100:  "kCreateCollection",
	101:  "kDropCollection",
	102:  "kHasCollection",
	103:  "kDescribeCollection",
	104:  "kShowCollections",
	105:  "kGetSysConfigs",
	106:  "kLoadCollection",
	107:  "kReleaseCollection",
	200:  "kCreatePartition",
	201:  "kDropPartition",
	202:  "kHasPartition",
	203:  "kDescribePartition",
	204:  "kShowPartitions",
	205:  "kLoadPartition",
	206:  "kReleasePartition",
	250:  "kShowSegment",
	251:  "kDescribeSegment",
	300:  "kCreateIndex",
	301:  "kDescribeIndex",
	302:  "kDropIndex",
	400:  "kInsert",
	401:  "kDelete",
	402:  "kFlush",
	500:  "kSearch",
	501:  "kSearchResult",
	502:  "kGetIndexState",
	503:  "kGetCollectionStatistics",
	504:  "kGetPartitionStatistics",
	600:  "kSegmentInfo",
	1200: "kTimeTick",
	1201: "kQueryNodeStats",
	1202: "kLoadIndex",
	1203: "kRequestID",
	1204: "kRequestTSO",
	1205: "kAllocateSegment",
	1206: "kSegmentStatistics",
	1207: "kSegmentFlushDone",
}

var MsgType_value = map[string]int32{
	"kNone":                    0,
	"kCreateCollection":        100,
	"kDropCollection":          101,
	"kHasCollection":           102,
	"kDescribeCollection":      103,
	"kShowCollections":         104,
	"kGetSysConfigs":           105,
	"kLoadCollection":          106,
	"kReleaseCollection":       107,
	"kCreatePartition":         200,
	"kDropPartition":           201,
	"kHasPartition":            202,
	"kDescribePartition":       203,
	"kShowPartitions":          204,
	"kLoadPartition":           205,
	"kReleasePartition":        206,
	"kShowSegment":             250,
	"kDescribeSegment":         251,
	"kCreateIndex":             300,
	"kDescribeIndex":           301,
	"kDropIndex":               302,
	"kInsert":                  400,
	"kDelete":                  401,
	"kFlush":                   402,
	"kSearch":                  500,
	"kSearchResult":            501,
	"kGetIndexState":           502,
	"kGetCollectionStatistics": 503,
	"kGetPartitionStatistics":  504,
	"kSegmentInfo":             600,
	"kTimeTick":                1200,
	"kQueryNodeStats":          1201,
	"kLoadIndex":               1202,
	"kRequestID":               1203,
	"kRequestTSO":              1204,
	"kAllocateSegment":         1205,
	"kSegmentStatistics":       1206,
	"kSegmentFlushDone":        1207,
}

func (x MsgType) String() string {
	return proto.EnumName(MsgType_name, int32(x))
}

func (MsgType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{3}
}

type Empty struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Empty) Reset()         { *m = Empty{} }
func (m *Empty) String() string { return proto.CompactTextString(m) }
func (*Empty) ProtoMessage()    {}
func (*Empty) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{0}
}

func (m *Empty) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Empty.Unmarshal(m, b)
}
func (m *Empty) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Empty.Marshal(b, m, deterministic)
}
func (m *Empty) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Empty.Merge(m, src)
}
func (m *Empty) XXX_Size() int {
	return xxx_messageInfo_Empty.Size(m)
}
func (m *Empty) XXX_DiscardUnknown() {
	xxx_messageInfo_Empty.DiscardUnknown(m)
}

var xxx_messageInfo_Empty proto.InternalMessageInfo

type Status struct {
	ErrorCode            ErrorCode `protobuf:"varint,1,opt,name=error_code,json=errorCode,proto3,enum=milvus.proto.common.ErrorCode" json:"error_code,omitempty"`
	Reason               string    `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *Status) Reset()         { *m = Status{} }
func (m *Status) String() string { return proto.CompactTextString(m) }
func (*Status) ProtoMessage()    {}
func (*Status) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{1}
}

func (m *Status) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Status.Unmarshal(m, b)
}
func (m *Status) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Status.Marshal(b, m, deterministic)
}
func (m *Status) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Status.Merge(m, src)
}
func (m *Status) XXX_Size() int {
	return xxx_messageInfo_Status.Size(m)
}
func (m *Status) XXX_DiscardUnknown() {
	xxx_messageInfo_Status.DiscardUnknown(m)
}

var xxx_messageInfo_Status proto.InternalMessageInfo

func (m *Status) GetErrorCode() ErrorCode {
	if m != nil {
		return m.ErrorCode
	}
	return ErrorCode_SUCCESS
}

func (m *Status) GetReason() string {
	if m != nil {
		return m.Reason
	}
	return ""
}

type KeyValuePair struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value                string   `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyValuePair) Reset()         { *m = KeyValuePair{} }
func (m *KeyValuePair) String() string { return proto.CompactTextString(m) }
func (*KeyValuePair) ProtoMessage()    {}
func (*KeyValuePair) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{2}
}

func (m *KeyValuePair) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyValuePair.Unmarshal(m, b)
}
func (m *KeyValuePair) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyValuePair.Marshal(b, m, deterministic)
}
func (m *KeyValuePair) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyValuePair.Merge(m, src)
}
func (m *KeyValuePair) XXX_Size() int {
	return xxx_messageInfo_KeyValuePair.Size(m)
}
func (m *KeyValuePair) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyValuePair.DiscardUnknown(m)
}

var xxx_messageInfo_KeyValuePair proto.InternalMessageInfo

func (m *KeyValuePair) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyValuePair) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

type Blob struct {
	Value                []byte   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Blob) Reset()         { *m = Blob{} }
func (m *Blob) String() string { return proto.CompactTextString(m) }
func (*Blob) ProtoMessage()    {}
func (*Blob) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{3}
}

func (m *Blob) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Blob.Unmarshal(m, b)
}
func (m *Blob) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Blob.Marshal(b, m, deterministic)
}
func (m *Blob) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Blob.Merge(m, src)
}
func (m *Blob) XXX_Size() int {
	return xxx_messageInfo_Blob.Size(m)
}
func (m *Blob) XXX_DiscardUnknown() {
	xxx_messageInfo_Blob.DiscardUnknown(m)
}

var xxx_messageInfo_Blob proto.InternalMessageInfo

func (m *Blob) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

type Address struct {
	Ip                   string   `protobuf:"bytes,1,opt,name=ip,proto3" json:"ip,omitempty"`
	Port                 int64    `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Address) Reset()         { *m = Address{} }
func (m *Address) String() string { return proto.CompactTextString(m) }
func (*Address) ProtoMessage()    {}
func (*Address) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{4}
}

func (m *Address) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Address.Unmarshal(m, b)
}
func (m *Address) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Address.Marshal(b, m, deterministic)
}
func (m *Address) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Address.Merge(m, src)
}
func (m *Address) XXX_Size() int {
	return xxx_messageInfo_Address.Size(m)
}
func (m *Address) XXX_DiscardUnknown() {
	xxx_messageInfo_Address.DiscardUnknown(m)
}

var xxx_messageInfo_Address proto.InternalMessageInfo

func (m *Address) GetIp() string {
	if m != nil {
		return m.Ip
	}
	return ""
}

func (m *Address) GetPort() int64 {
	if m != nil {
		return m.Port
	}
	return 0
}

type MsgBase struct {
	MsgType              MsgType  `protobuf:"varint,1,opt,name=msg_type,json=msgType,proto3,enum=milvus.proto.common.MsgType" json:"msg_type,omitempty"`
	MsgID                int64    `protobuf:"varint,2,opt,name=msgID,proto3" json:"msgID,omitempty"`
	Timestamp            uint64   `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	SourceID             int64    `protobuf:"varint,4,opt,name=sourceID,proto3" json:"sourceID,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MsgBase) Reset()         { *m = MsgBase{} }
func (m *MsgBase) String() string { return proto.CompactTextString(m) }
func (*MsgBase) ProtoMessage()    {}
func (*MsgBase) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{5}
}

func (m *MsgBase) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MsgBase.Unmarshal(m, b)
}
func (m *MsgBase) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MsgBase.Marshal(b, m, deterministic)
}
func (m *MsgBase) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MsgBase.Merge(m, src)
}
func (m *MsgBase) XXX_Size() int {
	return xxx_messageInfo_MsgBase.Size(m)
}
func (m *MsgBase) XXX_DiscardUnknown() {
	xxx_messageInfo_MsgBase.DiscardUnknown(m)
}

var xxx_messageInfo_MsgBase proto.InternalMessageInfo

func (m *MsgBase) GetMsgType() MsgType {
	if m != nil {
		return m.MsgType
	}
	return MsgType_kNone
}

func (m *MsgBase) GetMsgID() int64 {
	if m != nil {
		return m.MsgID
	}
	return 0
}

func (m *MsgBase) GetTimestamp() uint64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *MsgBase) GetSourceID() int64 {
	if m != nil {
		return m.SourceID
	}
	return 0
}

// Don't Modify This. @czs
type MsgHeader struct {
	Base                 *MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MsgHeader) Reset()         { *m = MsgHeader{} }
func (m *MsgHeader) String() string { return proto.CompactTextString(m) }
func (*MsgHeader) ProtoMessage()    {}
func (*MsgHeader) Descriptor() ([]byte, []int) {
	return fileDescriptor_555bd8c177793206, []int{6}
}

func (m *MsgHeader) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MsgHeader.Unmarshal(m, b)
}
func (m *MsgHeader) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MsgHeader.Marshal(b, m, deterministic)
}
func (m *MsgHeader) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MsgHeader.Merge(m, src)
}
func (m *MsgHeader) XXX_Size() int {
	return xxx_messageInfo_MsgHeader.Size(m)
}
func (m *MsgHeader) XXX_DiscardUnknown() {
	xxx_messageInfo_MsgHeader.DiscardUnknown(m)
}

var xxx_messageInfo_MsgHeader proto.InternalMessageInfo

func (m *MsgHeader) GetBase() *MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func init() {
	proto.RegisterEnum("milvus.proto.common.ErrorCode", ErrorCode_name, ErrorCode_value)
	proto.RegisterEnum("milvus.proto.common.IndexState", IndexState_name, IndexState_value)
	proto.RegisterEnum("milvus.proto.common.SegmentState", SegmentState_name, SegmentState_value)
	proto.RegisterEnum("milvus.proto.common.MsgType", MsgType_name, MsgType_value)
	proto.RegisterType((*Empty)(nil), "milvus.proto.common.Empty")
	proto.RegisterType((*Status)(nil), "milvus.proto.common.Status")
	proto.RegisterType((*KeyValuePair)(nil), "milvus.proto.common.KeyValuePair")
	proto.RegisterType((*Blob)(nil), "milvus.proto.common.Blob")
	proto.RegisterType((*Address)(nil), "milvus.proto.common.Address")
	proto.RegisterType((*MsgBase)(nil), "milvus.proto.common.MsgBase")
	proto.RegisterType((*MsgHeader)(nil), "milvus.proto.common.MsgHeader")
}

func init() { proto.RegisterFile("common.proto", fileDescriptor_555bd8c177793206) }

var fileDescriptor_555bd8c177793206 = []byte{
	// 1222 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x55, 0xdb, 0x6e, 0xdb, 0xc6,
	0x16, 0x8d, 0x2e, 0xb6, 0xcc, 0x2d, 0x45, 0x1e, 0x8f, 0x6f, 0x3a, 0xe7, 0xf8, 0x14, 0x81, 0x9f,
	0x02, 0x03, 0xb1, 0x8b, 0x16, 0x68, 0x9f, 0x02, 0x54, 0x26, 0x47, 0x36, 0x11, 0x8a, 0x54, 0x86,
	0x54, 0x9a, 0xb4, 0x0f, 0x04, 0x25, 0x4d, 0x64, 0x56, 0x94, 0xa8, 0x72, 0x46, 0x49, 0x94, 0xaf,
	0x68, 0x03, 0xf4, 0x2f, 0xda, 0xa2, 0xf7, 0xf6, 0x13, 0x7a, 0x7f, 0xee, 0x27, 0xf4, 0x03, 0x7a,
	0x43, 0xfb, 0x52, 0xcc, 0x90, 0x94, 0x88, 0x22, 0x7d, 0xe3, 0x5e, 0x7b, 0xf6, 0x9a, 0xbd, 0xd6,
	0xec, 0xe1, 0x40, 0x63, 0x18, 0x4f, 0xa7, 0xf1, 0xec, 0x74, 0x9e, 0xc4, 0x22, 0xc6, 0xbb, 0xd3,
	0x30, 0x7a, 0xb4, 0xe0, 0x69, 0x74, 0x9a, 0xa6, 0x8e, 0x6b, 0xb0, 0x41, 0xa6, 0x73, 0xb1, 0x3c,
	0xf6, 0x61, 0xd3, 0x15, 0x81, 0x58, 0x70, 0x7c, 0x1b, 0x80, 0x25, 0x49, 0x9c, 0xf8, 0xc3, 0x78,
	0xc4, 0x5a, 0xa5, 0x1b, 0xa5, 0x9b, 0xcd, 0x97, 0x5e, 0x38, 0x7d, 0x4e, 0xf1, 0x29, 0x91, 0xcb,
	0xf4, 0x78, 0xc4, 0xa8, 0xc6, 0xf2, 0x4f, 0x7c, 0x00, 0x9b, 0x09, 0x0b, 0x78, 0x3c, 0x6b, 0x95,
	0x6f, 0x94, 0x6e, 0x6a, 0x34, 0x8b, 0x8e, 0x5f, 0x81, 0xc6, 0x1d, 0xb6, 0xbc, 0x17, 0x44, 0x0b,
	0xd6, 0x0b, 0xc2, 0x04, 0x23, 0xa8, 0x4c, 0xd8, 0x52, 0xf1, 0x6b, 0x54, 0x7e, 0xe2, 0x3d, 0xd8,
	0x78, 0x24, 0xd3, 0x59, 0x61, 0x1a, 0x1c, 0x1f, 0x41, 0xf5, 0x3c, 0x8a, 0x07, 0xeb, 0xac, 0xac,
	0x68, 0xe4, 0xd9, 0x5b, 0x50, 0x6b, 0x8f, 0x46, 0x09, 0xe3, 0x1c, 0x37, 0xa1, 0x1c, 0xce, 0x33,
	0xbe, 0x72, 0x38, 0xc7, 0x18, 0xaa, 0xf3, 0x38, 0x11, 0x8a, 0xad, 0x42, 0xd5, 0xf7, 0xf1, 0xb3,
	0x12, 0xd4, 0xba, 0x7c, 0x7c, 0x1e, 0x70, 0x86, 0x5f, 0x85, 0xad, 0x29, 0x1f, 0xfb, 0x62, 0x39,
	0xcf, 0x55, 0x1e, 0x3d, 0x57, 0x65, 0x97, 0x8f, 0xbd, 0xe5, 0x9c, 0xd1, 0xda, 0x34, 0xfd, 0x90,
	0x9d, 0x4c, 0xf9, 0xd8, 0x34, 0x32, 0xe6, 0x34, 0xc0, 0x47, 0xa0, 0x89, 0x70, 0xca, 0xb8, 0x08,
	0xa6, 0xf3, 0x56, 0xe5, 0x46, 0xe9, 0x66, 0x95, 0xae, 0x01, 0xfc, 0x5f, 0xd8, 0xe2, 0xf1, 0x22,
	0x19, 0x32, 0xd3, 0x68, 0x55, 0x55, 0xd9, 0x2a, 0x3e, 0xbe, 0x0d, 0x5a, 0x97, 0x8f, 0x2f, 0x59,
	0x30, 0x62, 0x09, 0x7e, 0x11, 0xaa, 0x83, 0x80, 0xa7, 0x1d, 0xd5, 0xff, 0xbd, 0x23, 0xa9, 0x80,
	0xaa, 0x95, 0x27, 0x5f, 0x55, 0x41, 0x5b, 0x9d, 0x04, 0xae, 0x43, 0xcd, 0xed, 0xeb, 0x3a, 0x71,
	0x5d, 0x74, 0x0d, 0xef, 0x01, 0xea, 0xdb, 0xe4, 0x7e, 0x8f, 0xe8, 0x1e, 0x31, 0x7c, 0x42, 0xa9,
	0x43, 0x51, 0x09, 0x63, 0x68, 0xea, 0x8e, 0x6d, 0x13, 0xdd, 0xf3, 0x3b, 0x6d, 0xd3, 0x22, 0x06,
	0x2a, 0xe3, 0x7d, 0xd8, 0xe9, 0x11, 0xda, 0x35, 0x5d, 0xd7, 0x74, 0x6c, 0xdf, 0x20, 0xb6, 0x49,
	0x0c, 0x54, 0xc1, 0xff, 0x81, 0x7d, 0xdd, 0xb1, 0x2c, 0xa2, 0x7b, 0x12, 0xb6, 0x1d, 0xcf, 0x27,
	0xf7, 0x4d, 0xd7, 0x73, 0x51, 0x55, 0x72, 0x9b, 0x96, 0x45, 0x2e, 0xda, 0x96, 0xdf, 0xa6, 0x17,
	0xfd, 0x2e, 0xb1, 0x3d, 0xb4, 0x21, 0x79, 0x72, 0xd4, 0x30, 0xbb, 0xc4, 0x96, 0x74, 0xa8, 0x86,
	0x0f, 0x00, 0xe7, 0xb0, 0x69, 0x1b, 0xe4, 0xbe, 0xef, 0x3d, 0xe8, 0x11, 0xb4, 0x85, 0xff, 0x07,
	0x87, 0x39, 0x5e, 0xdc, 0xa7, 0xdd, 0x25, 0x48, 0xc3, 0x08, 0x1a, 0x79, 0xd2, 0x73, 0x7a, 0x77,
	0x10, 0x14, 0xd9, 0xa9, 0xf3, 0x3a, 0x25, 0xba, 0x43, 0x0d, 0x54, 0x2f, 0xc2, 0xf7, 0x88, 0xee,
	0x39, 0xd4, 0x37, 0x0d, 0xd4, 0x90, 0xcd, 0xe7, 0xb0, 0x4b, 0xda, 0x54, 0xbf, 0xf4, 0x29, 0x71,
	0xfb, 0x96, 0x87, 0xae, 0x4b, 0x0b, 0x3a, 0xa6, 0x45, 0x94, 0xa2, 0x8e, 0xd3, 0xb7, 0x0d, 0xd4,
	0xc4, 0xdb, 0x50, 0xef, 0x12, 0xaf, 0x9d, 0x7b, 0xb2, 0x2d, 0xf7, 0xd7, 0xdb, 0xfa, 0x25, 0xc9,
	0x11, 0x84, 0x5b, 0xb0, 0xa7, 0xb7, 0x6d, 0x59, 0xa4, 0x53, 0xd2, 0xf6, 0x88, 0xdf, 0x71, 0x2c,
	0x83, 0x50, 0xb4, 0x23, 0x05, 0xfe, 0x23, 0x63, 0x5a, 0x04, 0xe1, 0x42, 0x85, 0x41, 0x2c, 0xb2,
	0xae, 0xd8, 0x2d, 0x54, 0xe4, 0x19, 0x59, 0xb1, 0x27, 0xc5, 0x9c, 0xf7, 0x4d, 0xcb, 0xc8, 0x8c,
	0x4a, 0x0f, 0x6d, 0x1f, 0xef, 0xc0, 0xf5, 0x5c, 0x8c, 0x6d, 0x99, 0xae, 0x87, 0x0e, 0xf0, 0x21,
	0xec, 0xe6, 0x50, 0x97, 0x78, 0xd4, 0xd4, 0x53, 0x57, 0x0f, 0xe5, 0x5a, 0xa7, 0xef, 0xf9, 0x4e,
	0xc7, 0xef, 0x92, 0xae, 0x43, 0x1f, 0xa0, 0x16, 0xde, 0x83, 0x6d, 0xc3, 0xf0, 0x29, 0xb9, 0xdb,
	0x27, 0xae, 0xe7, 0xd3, 0xb6, 0x4e, 0xd0, 0xcf, 0xb5, 0x93, 0x37, 0x01, 0xcc, 0xd9, 0x88, 0x3d,
	0x91, 0x37, 0x9f, 0xe1, 0x2d, 0xa8, 0xda, 0x8e, 0x4d, 0xd0, 0x35, 0xdc, 0x80, 0xad, 0xbe, 0x6d,
	0xba, 0x6e, 0x9f, 0x18, 0xa8, 0x84, 0x9b, 0x00, 0xa6, 0xdd, 0xa3, 0xce, 0x05, 0x95, 0x53, 0x55,
	0x96, 0xd9, 0x8e, 0x69, 0x9b, 0xee, 0xa5, 0x1a, 0x11, 0x80, 0xcd, 0xcc, 0x9f, 0xaa, 0x1c, 0xbe,
	0x54, 0x8c, 0x81, 0x36, 0x4e, 0x62, 0x68, 0xb8, 0x6c, 0x3c, 0x65, 0x33, 0x91, 0xd2, 0x6f, 0x43,
	0x3d, 0x8b, 0xed, 0x78, 0xc6, 0xd0, 0x35, 0xbc, 0x0b, 0xdb, 0x2b, 0x40, 0x90, 0x27, 0x21, 0x17,
	0xe9, 0x70, 0x66, 0xe0, 0x45, 0x12, 0x3f, 0x0e, 0x67, 0x63, 0x54, 0x96, 0x7a, 0x72, 0x26, 0x16,
	0x44, 0x6c, 0x84, 0x2a, 0x85, 0x65, 0x9d, 0x68, 0xc1, 0xaf, 0xd8, 0x08, 0x55, 0x4f, 0xde, 0xdb,
	0x54, 0x97, 0x5b, 0xdd, 0x51, 0x0d, 0x36, 0x26, 0xd9, 0x36, 0xfb, 0xb0, 0x33, 0xd1, 0x13, 0x16,
	0x08, 0xa6, 0xc7, 0x51, 0xc4, 0x86, 0x22, 0x8c, 0x67, 0x68, 0x24, 0x77, 0x9f, 0x18, 0x49, 0x3c,
	0x2f, 0x80, 0x4c, 0xd2, 0x4e, 0x2e, 0x03, 0x5e, 0xc0, 0x1e, 0x4a, 0x9b, 0x27, 0x06, 0xe3, 0xc3,
	0x24, 0x1c, 0x14, 0x19, 0xc6, 0xf2, 0x06, 0x4c, 0xdc, 0xab, 0xf8, 0xf1, 0x1a, 0xe4, 0xe8, 0x4a,
	0x51, 0x5c, 0x30, 0xe1, 0x2e, 0xb9, 0x1e, 0xcf, 0x1e, 0x86, 0x63, 0x8e, 0x42, 0xb5, 0x97, 0x15,
	0x07, 0xa3, 0x42, 0xf9, 0x5b, 0x72, 0x00, 0x26, 0x94, 0x45, 0x2c, 0xe0, 0x45, 0xda, 0x09, 0xde,
	0x07, 0x94, 0xf5, 0xdb, 0x0b, 0x12, 0x11, 0x2a, 0xf4, 0xeb, 0x12, 0xde, 0x85, 0xa6, 0xea, 0x77,
	0x0d, 0x7e, 0x23, 0xdd, 0xba, 0x2e, 0xfb, 0x5d, 0x63, 0xdf, 0x96, 0xf0, 0x21, 0xe0, 0x55, 0xbf,
	0xeb, 0xc4, 0x77, 0x25, 0x39, 0x03, 0xaa, 0xdf, 0x15, 0xc8, 0xd1, 0xf7, 0x29, 0xaf, 0xec, 0x6d,
	0xbd, 0xf4, 0x87, 0x12, 0x3e, 0x80, 0x9d, 0xbc, 0xb7, 0x35, 0xfe, 0x63, 0x09, 0xef, 0x40, 0x43,
	0x51, 0x64, 0xde, 0xa3, 0x3f, 0x4b, 0xaa, 0xdd, 0x7c, 0xbb, 0x1c, 0xfe, 0x2b, 0x5d, 0x99, 0xaa,
	0x50, 0x13, 0x86, 0xde, 0x2f, 0xa7, 0x0a, 0xb2, 0x95, 0x29, 0xf8, 0x41, 0x19, 0x6f, 0x03, 0x28,
	0x59, 0x29, 0xf0, 0xa1, 0x9c, 0xae, 0xda, 0xc4, 0x9c, 0x71, 0x96, 0x08, 0xf4, 0x4e, 0x45, 0x45,
	0x06, 0x8b, 0x98, 0x60, 0xe8, 0xdd, 0x0a, 0xae, 0xc3, 0xe6, 0x44, 0x9d, 0x37, 0x7a, 0x96, 0xa6,
	0x5c, 0x16, 0x24, 0xc3, 0x2b, 0xf4, 0x4b, 0x45, 0x39, 0x91, 0x46, 0x94, 0xf1, 0x45, 0x24, 0xd0,
	0xaf, 0x15, 0xb5, 0xe1, 0x05, 0x13, 0xeb, 0x11, 0x47, 0xbf, 0x55, 0xf0, 0xff, 0xa1, 0x25, 0xc1,
	0xb5, 0xe5, 0x32, 0x13, 0x72, 0x11, 0x0e, 0x39, 0xfa, 0xbd, 0x82, 0x8f, 0xe0, 0x50, 0xa6, 0x57,
	0xaa, 0x0b, 0xd9, 0x3f, 0x2a, 0xa9, 0xfe, 0x54, 0xa4, 0x39, 0x7b, 0x18, 0xa3, 0x9f, 0xaa, 0xb8,
	0x09, 0xda, 0xc4, 0x0b, 0xa7, 0xcc, 0x0b, 0x87, 0x13, 0xf4, 0x91, 0xa6, 0x5c, 0xbe, 0xbb, 0x60,
	0xc9, 0xd2, 0x8e, 0x47, 0x4c, 0x56, 0x73, 0xf4, 0xb1, 0xa6, 0x64, 0x4a, 0x97, 0x53, 0x99, 0x9f,
	0xa4, 0x00, 0x65, 0x6f, 0x2f, 0x18, 0x17, 0xa6, 0x81, 0x3e, 0x95, 0x7f, 0xbb, 0x7a, 0x0e, 0x78,
	0xae, 0x83, 0x3e, 0xd3, 0x94, 0xb3, 0xed, 0x28, 0x8a, 0x87, 0x81, 0x58, 0x39, 0xfb, 0xb9, 0xa6,
	0xce, 0xb7, 0x70, 0xb1, 0xb2, 0xe6, 0xbe, 0xd0, 0xd4, 0xa1, 0x15, 0x2f, 0x85, 0x21, 0xe7, 0xff,
	0x4b, 0xed, 0xfc, 0xfc, 0x8d, 0xd7, 0xc6, 0xa1, 0xb8, 0x5a, 0x0c, 0xe4, 0xfb, 0x71, 0xf6, 0x34,
	0x8c, 0xa2, 0xf0, 0xa9, 0x60, 0xc3, 0xab, 0xb3, 0xf4, 0x6d, 0xb9, 0x35, 0x0a, 0xb9, 0x48, 0xc2,
	0xc1, 0x42, 0xb0, 0xd1, 0x59, 0x38, 0x13, 0x2c, 0x99, 0x05, 0xd1, 0x99, 0x7a, 0x70, 0xce, 0xd2,
	0x07, 0x67, 0x3e, 0x18, 0x6c, 0xaa, 0xf8, 0xe5, 0xbf, 0x03, 0x00, 0x00, 0xff, 0xff, 0xb5, 0x3d,
	0x8c, 0x4b, 0x53, 0x08, 0x00, 0x00,
}
