package storage_v2

import (
	"context"

	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/apache/arrow/go/v8/arrow"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

// TODO: when to call retain and release

type BinlogReader interface {
	Read() *storage.InsertData
}

type ParquetBinlogReader struct {
	fieldSchemas map[string]*schemapb.FieldSchema
	fileReader   *pqarrow.FileReader
}

func (br *ParquetBinlogReader) Read() *storage.InsertData {
	// FIXME
	table, _ := br.fileReader.ReadTable(context.TODO())

	for i := 0; i < int(table.NumCols()); i++ {
		col := table.Column(i)
		field, _ := br.fieldSchemas[col.Name()]
		col.Data()

	}

}

func toFieldData(col arrow.Column, field *schemapb.FieldSchema) storage.FieldData {
	chunks := col.Data().Chunks()
	switch col.DataType().(type) {
	case *arrow.BooleanType:
		data := toRawData[bool, *array.Boolean](chunks)
		return &storage.BoolFieldData{
			Data: data,
		}
	case *arrow.Int8Type:
		data := toRawData[int8, *array.Int8](chunks)
		return &storage.Int8FieldData{
			Data: data,
		}
	case *arrow.Int16Type:
		data := toRawData[int16, *array.Int16](chunks)
		return &storage.Int16FieldData{
			Data: data,
		}
	case *arrow.Int32Type:
		data := toRawData[int32, *array.Int32](chunks)
		return &storage.Int32FieldData{
			Data: data,
		}
	case *arrow.Int64Type:
		data := toRawData[int64, *array.Int64](chunks)
		return &storage.Int64FieldData{
			Data: data,
		}
	case *arrow.Float32Type:
		data := toRawData[float32, *array.Float32](chunks)
		return &storage.FloatFieldData{
			Data: data,
		}
	case *arrow.Float64Type:
		data := toRawData[float64, *array.Float64](chunks)
		return &storage.DoubleFieldData{
			Data: data,
		}
	case *arrow.StringType:
		data := toRawData[string, *array.String](chunks)
		return &storage.StringFieldData{
			Data: data,
		}
	case *arrow.FixedSizeBinaryType:
		data := toRawData[[]byte, *array.FixedSizeBinary](chunks)
		if field.GetDataType() == schemapb.DataType_BinaryVector {
			return &storage.BinaryVectorFieldData{
				Data: data,
			}
		}
	default:
		panic("unknown arrow type")

	}
}

type arrowArray[T any] interface {
	Value(i int) T
	Len() int
}

func toRawData[T any, U arrowArray[T]](chunks []arrow.Array) []T {
	var res []T
	for _, chunk := range chunks {
		// FIXME
		real, _ := chunk.(U)
		for i := 0; i < real.Len(); i++ {
			res = append(res, real.Value(i))
		}
	}
	return res
}

func toRawVectorData(chunk []arrow.Array) []byte {
	var res []byte
	for _, chunk := range chunks {
		// FIXME
		real, _ := chunk.(U)
		for i := 0; i < real.Len(); i++ {
			res = append(res, real.Value(i))
		}
	}
	return res
}
