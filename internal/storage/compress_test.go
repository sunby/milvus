package storage

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

func generateInsertBinlogWriter(compressType CompressType) *InsertBinlogWriter {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
	w.setCompressType(compressType)
	return w
}

// 用于执行实验1
func process(data [][]int64, compressType CompressType, t *testing.T) (int64, [2]time.Duration, [2]time.Duration) {
	// 第一阶段：compress
	startTime_compress := time.Now()
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
	w.setCompressType(compressType)

	curTS := time.Now().UnixNano() / int64(time.Millisecond)
	e, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	for i := 0; i < len(data); i++ {
		err = e.AddDataToPayload(data[i])
		assert.Nil(t, err)
	}
	e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
	e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

	w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
	w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))
	elapsedTime_compress := time.Since(startTime_compress)

	// 第二阶段：写入IO
	w.Close()
	buf, _ := w.GetBuffer()

	fd, _ := os.Create("/tmp/binlog_int64.db")
	fd.Write(buf)
	err = fd.Close()
	elapsedTime_compress_IO := time.Since(startTime_compress)

	// 第三阶段：读取IO
	startTime_decompress_IO := time.Now()
	fi, err := os.Stat("/tmp/binlog_int64.db")
	size := fi.Size()

	fr, _ := os.Open("/tmp/binlog_int64.db")
	buf = make([]byte, size)
	fr.Read(buf)

	// 第四阶段：Decompress
	startTime_decompress := time.Now()
	r, err := NewBinlogReader(buf)
	event1, err := r.NextEventReader()
	_, err = event1.GetInt64FromPayload()
	elapsedTime_decompress_IO := time.Since(startTime_decompress_IO)
	elapsedTime_decompress := time.Since(startTime_decompress)

	return size, [2]time.Duration{elapsedTime_compress, elapsedTime_compress_IO}, [2]time.Duration{elapsedTime_decompress, elapsedTime_decompress_IO}
}

// 实验一：测试使用单个 InsertEventWriter 插入不同规模的 int64 数据
func TestCompressBinlogFilesInt64(t *testing.T) {
	rows := [9]int{10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000} // 测试数据条数
	dims := [5]int{10, 50, 100, 500, 1000}                             // 每条数据的维度

	rand.Seed(time.Now().Unix())

	// 每个数组记录两个元素：内存处理时间、内存处理&磁盘IO时间
	compress_time_total_uncompressed := [2]time.Duration{time.Duration(0), time.Duration(0)}
	compress_time_total_snappy := [2]time.Duration{time.Duration(0), time.Duration(0)}
	compress_time_total_gzip := [2]time.Duration{time.Duration(0), time.Duration(0)}
	decompress_time_total_uncompressed := [2]time.Duration{time.Duration(0), time.Duration(0)}
	decompress_time_total_snappy := [2]time.Duration{time.Duration(0), time.Duration(0)}
	decompress_time_total_gzip := [2]time.Duration{time.Duration(0), time.Duration(0)}

	for m := 0; m < len(rows); m++ {
		for n := 0; n < len(dims); n++ {
			row := rows[m]
			dim := dims[n]
			fmt.Println("rows = ", row, ", dim = ", dim)
			var data [][]int64 // 存储测试数据

			for i := 0; i < row; i++ {
				line := make([]int64, dim)
				for j := 0; j < dim; j++ {
					line[j] = rand.Int63n(10000)
				}
				data = append(data, line)
			}

			size_uncompressed, compress_time_uncompressed, decompress_time_uncompressed := process(data, CompressType_UNCOMPRESSED, t)
			size_snappy, compress_time_snappy, decompress_time_snappy := process(data, CompressType_SNAPPY, t)
			size_gzip, compress_time_gzip, decompress_time_gzip := process(data, CompressType_GZIP, t)

			for i := 0; i < 2; i++ {
				compress_time_total_uncompressed[i] += compress_time_uncompressed[i]
				compress_time_total_snappy[i] += compress_time_snappy[i]
				compress_time_total_gzip[i] += compress_time_gzip[i]
				decompress_time_total_uncompressed[i] += decompress_time_uncompressed[i]
				decompress_time_total_snappy[i] += decompress_time_snappy[i]
				decompress_time_total_gzip[i] += decompress_time_gzip[i]
			}

			fmt.Println("Uncompressed size: ", size_uncompressed, " , compress time used: ", compress_time_uncompressed[0], " , compress(with IO) time used: ", compress_time_uncompressed[1], " decompress time used: ", decompress_time_uncompressed[0], " , decompress(with IO) time used: ", decompress_time_uncompressed[1])
			fmt.Println("Snappy size: ", size_snappy, " rate = ", float64(size_snappy)/float64(size_uncompressed), " , compress time used: ", compress_time_snappy[0], " , compress(with IO) time used: ", compress_time_snappy[1], " decompress time used: ", decompress_time_snappy[0], " , decompress(with IO) time used: ", decompress_time_snappy[1])
			fmt.Println("Gzip size: ", size_gzip, " rate = ", float64(size_gzip)/float64(size_uncompressed), " , compress time used: ", compress_time_gzip[0], " , compress(with IO) time used: ", compress_time_gzip[1], " decompress time used: ", decompress_time_gzip[0], " , decompress(with IO) time used: ", decompress_time_gzip[1])
		}
	}

	fmt.Println("Total compression time used of Uncompressed: ", compress_time_total_uncompressed[0])
	fmt.Println("Total compression time used of Snappy: ", compress_time_total_snappy[0])
	fmt.Println("Total compression time used of Gzip: ", compress_time_total_gzip[0])
	fmt.Println("Total decompression time used of Uncompressed: ", decompress_time_total_uncompressed[0])
	fmt.Println("Total decompression time used of Snappy: ", decompress_time_total_snappy[0])
	fmt.Println("Total decompression time used of Gzip: ", decompress_time_total_gzip[0])
	fmt.Println("Total compression time(with IO) used of Uncompressed: ", compress_time_total_uncompressed[1])
	fmt.Println("Total compression time(with IO) used of Snappy: ", compress_time_total_snappy[1])
	fmt.Println("Total compression time(with IO) used of Gzip: ", compress_time_total_gzip[1])
	fmt.Println("Total decompression time(with IO) used of Uncompressed: ", decompress_time_total_uncompressed[1])
	fmt.Println("Total decompression time(with IO) used of Snappy: ", decompress_time_total_snappy[1])
	fmt.Println("Total decompression time(with IO) used of Gzip: ", decompress_time_total_gzip[1])
}

// 用于执行实验2
func process2(eventLength int, data [][]int64, compressType CompressType, t *testing.T) int64 {
	w := generateInsertBinlogWriter(compressType)

	// add events
	curTS := time.Now().UnixNano() / int64(time.Millisecond)
	for j := 0; j < eventLength; j++ {
		e, err := w.NextInsertEventWriter()
		assert.Nil(t, err)
		for i := 0; i < len(data); i++ {
			err = e.AddDataToPayload(data[i])
			assert.Nil(t, err)
		}
		e.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
		e.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))
	}

	w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
	w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))
	_, err := w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)
	fd, err := os.Create("/tmp/binlog_int64.db")
	assert.Nil(t, err)
	num, err := fd.Write(buf)
	assert.Nil(t, err)
	assert.Equal(t, num, len(buf))
	err = fd.Close()
	assert.Nil(t, err)
	fi, err := os.Stat("/tmp/binlog_int64.db")
	assert.Nil(t, err)
	size := fi.Size()
	return size
}

// 实验二：测试使用多个 InsertEventWriter 插入相同规模的 int64 数据
func TestCompressBinlogFilesInt64WithMultiEvents(t *testing.T) {
	dim := 100
	row := 100
	events := [5]int{1, 10, 100, 1000, 10000}
	rand.Seed(time.Now().Unix())
	for n := 0; n < len(events); n++ {
		var data [][]int64
		for i := 0; i < row; i++ {
			line := make([]int64, dim)
			for j := 0; j < dim; j++ {
				line[j] = rand.Int63n(10000)
			}
			data = append(data, line)
		}
		res_uncompressed := process2(events[n], data, CompressType_UNCOMPRESSED, t)
		res_snappy := process2(events[n], data, CompressType_SNAPPY, t)
		res_gzip := process2(events[n], data, CompressType_GZIP, t)

		fmt.Println("Uncompressed size: ", res_uncompressed)
		fmt.Println("Snappy size: ", res_snappy, " rate = ", float64(res_snappy)/float64(res_uncompressed))
		fmt.Println("Gzip size: ", res_gzip, " rate = ", float64(res_gzip)/float64(res_uncompressed))
	}
}

// 用于执行实验3
func process3(Schema *etcdpb.CollectionMeta, insertDataFirst *InsertData, compressType CompressType, t *testing.T) {

	insertCodec := NewInsertCodec(Schema)
	// Uncompressed
	insertCodec.setCompressType(compressType)
	firstBlobs, _, err := insertCodec.Serialize(1, 1, insertDataFirst)
	assert.Nil(t, err)
	var uncompressedSize []int64
	for index, blob := range firstBlobs {
		blob.Key = fmt.Sprintf("1/insert_log/2/3/4/5/%d", 100)
		fileName := fmt.Sprintf("/tmp/firstblob_%d.db", index)
		fd, err := os.Create(fileName)
		assert.Nil(t, err)
		num, err := fd.Write(blob.GetValue())
		assert.Nil(t, err)
		assert.Equal(t, num, len(blob.GetValue()))
		err = fd.Close()
		assert.Nil(t, err)
		fi, err := os.Stat(fileName)
		assert.Nil(t, err)
		uncompressedSize = append(uncompressedSize, fi.Size())
	}
	fmt.Println(uncompressedSize)
}

// 实验三：BinlogFiles中的 insertData 压缩率测试
func TestCompressBinlogFiles(t *testing.T) {

	rows := [5]int{10, 100, 1000, 10000, 100000}

	for i := 0; i < len(rows); i++ {

		fmt.Println("Round ", i)
		Schema := &etcdpb.CollectionMeta{
			ID:            1,
			CreateTime:    1,
			SegmentIDs:    []int64{0, 1},
			PartitionTags: []string{"partition_0"},
			Schema: &schemapb.CollectionSchema{
				Name:        "schema",
				Description: "schema",
				AutoID:      true,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      0,
						Name:         "row_id",
						IsPrimaryKey: false,
						Description:  "row_id",
						DataType:     schemapb.DataType_Int64,
					},
					{
						FieldID:      1,
						Name:         "Ts",
						IsPrimaryKey: false,
						Description:  "Ts",
						DataType:     schemapb.DataType_Int64,
					},
					{
						FieldID:      100,
						Name:         "field_bool",
						IsPrimaryKey: false,
						Description:  "description_2",
						DataType:     schemapb.DataType_Bool,
					},
					{
						FieldID:      101,
						Name:         "field_int8",
						IsPrimaryKey: false,
						Description:  "description_3",
						DataType:     schemapb.DataType_Int8,
					},
					{
						FieldID:      102,
						Name:         "field_int16",
						IsPrimaryKey: false,
						Description:  "description_4",
						DataType:     schemapb.DataType_Int16,
					},
					{
						FieldID:      103,
						Name:         "field_int32",
						IsPrimaryKey: false,
						Description:  "description_5",
						DataType:     schemapb.DataType_Int32,
					},
					{
						FieldID:      104,
						Name:         "field_int64",
						IsPrimaryKey: false,
						Description:  "description_6",
						DataType:     schemapb.DataType_Int64,
					},
					{
						FieldID:      105,
						Name:         "field_float",
						IsPrimaryKey: false,
						Description:  "description_7",
						DataType:     schemapb.DataType_Float,
					},
					{
						FieldID:      106,
						Name:         "field_double",
						IsPrimaryKey: false,
						Description:  "description_8",
						DataType:     schemapb.DataType_Double,
					},
					{
						FieldID:      107,
						Name:         "field_string",
						IsPrimaryKey: false,
						Description:  "description_9",
						DataType:     schemapb.DataType_String,
					},
					{
						FieldID:      108,
						Name:         "field_binary_vector",
						IsPrimaryKey: false,
						Description:  "description_10",
						DataType:     schemapb.DataType_BinaryVector,
					},
					{
						FieldID:      109,
						Name:         "field_float_vector",
						IsPrimaryKey: false,
						Description:  "description_11",
						DataType:     schemapb.DataType_FloatVector,
					},
				},
			},
		}

		var data_int64 []int64
		var data_int64_ts []int64
		var data_int8 []int8
		var data_int16 []int16
		var data_int32 []int32
		var data_bool []bool
		var data_float32 []float32
		var data_float64 []float64
		var data_string []string
		var data_byte []byte

		for j := 0; j < rows[i]; j++ {
			data_int64 = append(data_int64, int64(j))
			data_int64_ts = append(data_int64_ts, int64(1))
			data_bool = append(data_bool, j%2 == 0)
			data_int8 = append(data_int8, int8(j))
			data_int16 = append(data_int16, int16(j))
			data_int32 = append(data_int32, int32(j))
			data_float32 = append(data_float32, float32(j))
			data_float64 = append(data_float64, float64(j))
			data_string = append(data_string, strconv.FormatInt(int64(j), 10))
			data_byte = append(data_byte, byte(j%255))
		}

		fmt.Println(len(data_byte))

		insertData := &InsertData{
			Data: map[int64]FieldData{
				0: &Int64FieldData{ // DataType_Int64
					NumRows: len(data_int64),
					Data:    data_int64,
				},
				1: &Int64FieldData{ // DataType_Int64
					NumRows: len(data_int64_ts),
					Data:    data_int64_ts,
				},
				100: &BoolFieldData{ // DataType_Bool
					NumRows: len(data_bool),
					Data:    data_bool,
				},
				101: &Int8FieldData{ // DataType_Int8
					NumRows: len(data_int8),
					Data:    data_int8,
				},
				102: &Int16FieldData{ // DataType_Int16
					NumRows: len(data_int16),
					Data:    data_int16,
				},
				103: &Int32FieldData{ // DataType_Int32
					NumRows: len(data_int32),
					Data:    data_int32,
				},
				104: &Int64FieldData{ // DataType_Int64
					NumRows: len(data_int64),
					Data:    data_int64,
				},
				105: &FloatFieldData{ // DataType_Float
					NumRows: len(data_float32),
					Data:    data_float32,
				},
				106: &DoubleFieldData{ // DataType_Double
					NumRows: len(data_float64),
					Data:    data_float64,
				},
				107: &StringFieldData{ // DataType_String
					NumRows: len(data_string),
					Data:    data_string,
				},
				108: &BinaryVectorFieldData{ // DataType_BinaryVector
					NumRows: len(data_byte),
					Data:    data_byte, //[]byte{0, 255, 255, 0, 125, 100},
					Dim:     8,
				},
				109: &FloatVectorFieldData{ // DataType_FloatVector
					NumRows: len(data_float32) / 10,
					Data:    data_float32,
					Dim:     10,
				},
			},
		}
		process3(Schema, insertData, CompressType_UNCOMPRESSED, t)
		process3(Schema, insertData, CompressType_SNAPPY, t)
		process3(Schema, insertData, CompressType_GZIP, t)
	}
}
