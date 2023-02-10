package storage_v2

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

var encodingEndian = binary.LittleEndian

type WriteOptions struct {
	MaxRowGroupSize int64
	ChunkSize       int64
}

type BinlogWriter interface {
	Write(data *storage.InsertData)
	Close()
}

type ParquetBinlogWriter struct {
	fieldSchemas []*schemapb.FieldSchema
	arrowSchema  *arrow.Schema
	writer       io.Writer
	options      WriteOptions
	file         *pqarrow.FileWriter
}

func (bw *ParquetBinlogWriter) Write(data *storage.InsertData) {
	table := bw.createArrowTable(data)
	if err := bw.file.WriteTable(table, bw.options.ChunkSize); err != nil {
		// FIXME
		panic("failed to write table")
	}
}

func (bw *ParquetBinlogWriter) createArrowTable(data *storage.InsertData) arrow.Table {
	var columns []arrow.Column
	rowNum := -1
	for i, f := range bw.fieldSchemas {
		columns = append(columns, toArrowColumn(f, bw.arrowSchema.Fields()[i], data.Data[f.GetFieldID()]))
		// FIXME
		rowNum = data.Data[f.GetFieldID()].RowNum()
	}

	return array.NewTable(bw.arrowSchema, columns, int64(rowNum))
}

func (bw *ParquetBinlogWriter) Close() {
	if err := bw.file.Close(); err != nil {
		// FIXME
		panic("failed to close file")
	}

}

func NewParquetBinlogWriter(
	fieldSchemas []*schemapb.FieldSchema,
	writer io.Writer,
	options WriteOptions) *ParquetBinlogWriter {
	arrowSchema := toArrowSchema(fieldSchemas)
	// FIXME
	fileWriter, err := pqarrow.NewFileWriter(arrowSchema, writer, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		// FIXME
		panic("failed to init file writer")
	}
	return &ParquetBinlogWriter{
		fieldSchemas: fieldSchemas,
		arrowSchema:  arrowSchema,
		writer:       writer,
		options:      options,
		file:         fileWriter,
	}
}

func toArrowColumn(field *schemapb.FieldSchema, arrowField arrow.Field, fieldData storage.FieldData) arrow.Column {
	var builder array.Builder
	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		builder = array.NewBooleanBuilder(memory.DefaultAllocator)
		arrowAppend[bool, *array.BooleanBuilder](builder, fieldData)
	case schemapb.DataType_Int8:
		builder := array.NewInt8Builder(memory.DefaultAllocator)
		arrowAppend[int8, *array.Int8Builder](builder, fieldData)
	case schemapb.DataType_Int16:
		builder := array.NewInt16Builder(memory.DefaultAllocator)
		arrowAppend[int16, *array.Int16Builder](builder, fieldData)
	case schemapb.DataType_Int32:
		builder := array.NewInt32Builder(memory.DefaultAllocator)
		arrowAppend[int32, *array.Int32Builder](builder, fieldData)
	case schemapb.DataType_Int64:
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		arrowAppend[int64, *array.Int64Builder](builder, fieldData)
	case schemapb.DataType_Float:
		builder := array.NewFloat32Builder(memory.DefaultAllocator)
		arrowAppend[float32, *array.Float32Builder](builder, fieldData)
	case schemapb.DataType_Double:
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		arrowAppend[float64, *array.Float64Builder](builder, fieldData)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		arrowAppend[string, *array.StringBuilder](builder, fieldData)
	case schemapb.DataType_BinaryVector:
		// FIXME
		dim, _ := storage.GetDimFromParams(field.GetTypeParams())
		builder := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: dim / 8})
		arrowAppend[[]byte, *array.FixedSizeBinaryBuilder](builder, fieldData)
	case schemapb.DataType_FloatVector:
		// FIXME
		dim, _ := storage.GetDimFromParams(field.GetTypeParams())
		binaryBuilder := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: dim * 4})
		floatFieldData, _ := fieldData.(*storage.FloatVectorFieldData)
		arrowAppendFloatVector(binaryBuilder, floatFieldData)
	default:
		panic("unsupported type")
	}
	return arrow.NewColumnFromArr(arrowField, builder.NewArray())
}

type arrowBuilder[T any] interface {
	Append(data T)
}

func arrowAppend[T any, U arrowBuilder[T]](builder array.Builder, fieldData storage.FieldData) {
	// FIXME
	realBuilder, _ := builder.(U)
	for i := 0; i < fieldData.RowNum(); i++ {
		v, ok := fieldData.GetRow(i).(T)
		if !ok {
			// FIXME
			panic("type assertion failed")
		}
		realBuilder.Append(v)
	}
}

func arrowAppendFloatVector(builder *array.FixedSizeBinaryBuilder, fieldData *storage.FloatVectorFieldData) {
	for i := 0; i < fieldData.RowNum(); i++ {
		// FIXME
		data, _ := fieldData.GetRow(i).([]float32)
		dataInBytes := floatsToBytes(data)
		builder.Append(dataInBytes)
	}
}

func floatsToBytes(data []float32) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, encodingEndian, data)
	return buf.Bytes()
}

func toArrowSchema(fields []*schemapb.FieldSchema) *arrow.Schema {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, f := range fields {
		arrowFields = append(arrowFields, toArrowFiled(f))
	}
	return arrow.NewSchema(arrowFields, nil)

}

func toArrowFiled(field *schemapb.FieldSchema) arrow.Field {
	// FIXME: should use field name or ID?
	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		return arrow.Field{Name: field.GetName(), Type: &arrow.BooleanType{}, Nullable: true}
	case schemapb.DataType_Int8:
		return arrow.Field{Name: field.GetName(), Type: &arrow.Int8Type{}, Nullable: true}
	case schemapb.DataType_Int16:
		return arrow.Field{Name: field.GetName(), Type: &arrow.Int16Type{}, Nullable: true}
	case schemapb.DataType_Int32:
		return arrow.Field{Name: field.GetName(), Type: &arrow.Int32Type{}, Nullable: true}
	case schemapb.DataType_Int64:
		return arrow.Field{Name: field.GetName(), Type: &arrow.Int64Type{}, Nullable: true}
	case schemapb.DataType_Float:
		return arrow.Field{Name: field.GetName(), Type: &arrow.Float32Type{}, Nullable: true}
	case schemapb.DataType_Double:
		return arrow.Field{Name: field.GetName(), Type: &arrow.Float64Type{}, Nullable: true}
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return arrow.Field{Name: field.GetName(), Type: &arrow.StringType{}, Nullable: true}
	case schemapb.DataType_BinaryVector:
		// FIXME
		dim, _ := storage.GetDimFromParams(field.GetTypeParams())
		return arrow.Field{Name: field.GetName(), Type: &arrow.FixedSizeBinaryType{ByteWidth: dim / 8}, Nullable: true}
	case schemapb.DataType_FloatVector:
		// FIXME
		dim, _ := storage.GetDimFromParams(field.GetTypeParams())
		return arrow.Field{Name: field.GetName(), Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}, Nullable: true}
	default:
		panic("unsupported type")
	}
}

func WriteBinlogFile(fieldSchemas []*schemapb.FieldSchema, data *storage.InsertData, o io.Writer, options WriteOptions) {
}
