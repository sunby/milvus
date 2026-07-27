package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCurrentSplitForGrowingPackFillsNewSplitFormats(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataNodeCfg.StorageFormat.Key, "parquet"))
	t.Cleanup(func() {
		_ = params.Reset(params.DataNodeCfg.StorageFormat.Key)
	})

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
	}}
	meta := &streamingpb.SegmentAssignmentMeta{StorageVersion: storage.StorageV3}

	columnGroups := currentSplitForGrowingPack(schema, nil, meta)

	require.NotEmpty(t, columnGroups)
	for _, columnGroup := range columnGroups {
		assert.Equal(t, "parquet", columnGroup.Format)
	}
}

func TestCurrentSplitFromPersistedStorageRestoresFormat(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
	}}
	persisted := &streamingpb.L1SegmentPersistedStorage{
		Binlogs: []*streamingpb.L1SegmentBinLogs{{
			FieldBinlog: []*datapb.FieldBinlog{{
				FieldID:     101,
				ChildFields: []int64{101},
				Format:      "vortex",
			}},
		}},
	}

	columnGroups := currentSplitFromPersistedStorage(schema, persisted)

	require.Len(t, columnGroups, 1)
	assert.Equal(t, int64(101), columnGroups[0].GroupID)
	assert.Equal(t, []int64{101}, columnGroups[0].Fields)
	assert.Equal(t, []int{1}, columnGroups[0].Columns)
	assert.Equal(t, "vortex", columnGroups[0].Format)
}

func TestCurrentSplitFromPersistedStoragePreservesFormat(t *testing.T) {
	schema := testGrowingPackSchema()
	persistedStorage := &streamingpb.L1SegmentPersistedStorage{
		Binlogs: []*streamingpb.L1SegmentBinLogs{
			{
				FieldBinlog: []*datapb.FieldBinlog{
					{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
				},
			},
		},
	}

	currentSplit := currentSplitFromPersistedStorage(schema, persistedStorage)
	require.Len(t, currentSplit, 1)
	require.Equal(t, int64(0), currentSplit[0].GroupID)
	require.Equal(t, []int64{100, 0, 1}, currentSplit[0].Fields)
	require.Equal(t, []int{2, 0, 1}, currentSplit[0].Columns)
	require.Equal(t, "parquet", currentSplit[0].Format)
}

func TestCurrentSplitForNewGrowingPackFillsFormats(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
	}

	currentSplit := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NotEmpty(t, currentSplit)
	wantFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	for _, columnGroup := range currentSplit {
		require.Equal(t, wantFormat, columnGroup.Format)
	}
}

func TestCurrentSplitForGrowingPackPreservesPersistedManifestFormat(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: "manifest-v2",
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "vortex"},
					},
				},
			},
		},
	}

	currentSplit := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.Len(t, currentSplit, 1)
	require.Equal(t, "vortex", currentSplit[0].Format)
}

func TestCurrentSplitForGrowingPackDoesNotGuessPersistedV3Format(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: "manifest-v2",
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}},
					},
				},
			},
		},
	}

	currentSplit := currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.Len(t, currentSplit, 1)
	require.Empty(t, currentSplit[0].Format)
}

func TestCurrentSplitForGrowingPackIgnoresStorageV2(t *testing.T) {
	meta := &streamingpb.SegmentAssignmentMeta{StorageVersion: storage.StorageV2}
	require.Nil(t, currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta))
}

func testGrowingPackSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, DataType: schemapb.DataType_Int64},
			{FieldID: 1, DataType: schemapb.DataType_Int64},
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		},
	}
}

func TestNewBulkPackWriterUsesDefaultStorageConfig(t *testing.T) {
	paramtable.Init()

	writer := NewBulkPackWriter(nil, nil, nil)
	bulkWriter, ok := writer.(*growingBulkPackWriter)
	require.True(t, ok)
	require.NotNil(t, bulkWriter.storageConfig)
}
