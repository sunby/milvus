package segment

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	columnGroups, err := (&growingBulkPackWriter{}).currentSplitForGrowingPack(schema, nil, meta)

	require.NoError(t, err)
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
					{FieldID: 0, ChildFields: []int64{100, 0, 1}},
				},
			},
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
	writer := &growingBulkPackWriter{
		resolveManifestFormat: func(string, *indexpb.StorageConfig, []string, string) (string, error) {
			t.Fatal("new manifest must not resolve formats from object storage")
			return "", nil
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
	}

	currentSplit, err := writer.currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NoError(t, err)
	require.NotEmpty(t, currentSplit)
	wantFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	for _, columnGroup := range currentSplit {
		require.Equal(t, wantFormat, columnGroup.Format)
	}
}

func TestCurrentSplitForGrowingPackRecoversMissingManifestFormats(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("root/insert_log/1/2/3", 2)
	resolverCalls := 0
	writer := &growingBulkPackWriter{
		resolveManifestFormat: func(gotManifest string, _ *indexpb.StorageConfig, columns []string, fallback string) (string, error) {
			resolverCalls++
			require.Equal(t, manifestPath, gotManifest)
			require.Equal(t, []string{"100", "0", "1"}, columns)
			require.Empty(t, fallback)
			return "parquet", nil
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: manifestPath,
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}},
					},
				},
			},
		},
	}

	currentSplit, err := writer.currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NoError(t, err)
	require.Equal(t, 1, resolverCalls)
	require.Len(t, currentSplit, 1)
	require.Equal(t, "parquet", currentSplit[0].Format)
}

func TestCurrentSplitForGrowingPackPreservesPersistedManifestFormat(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("root/insert_log/1/2/3", 2)
	writer := &growingBulkPackWriter{
		resolveManifestFormat: func(string, *indexpb.StorageConfig, []string, string) (string, error) {
			t.Fatal("persisted format must be reused without reading the manifest")
			return "", nil
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: manifestPath,
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "vortex"},
					},
				},
			},
		},
	}

	currentSplit, err := writer.currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.NoError(t, err)
	require.Len(t, currentSplit, 1)
	require.Equal(t, "vortex", currentSplit[0].Format)
}

func TestCurrentSplitForGrowingPackRejectsMissingManifestFormat(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("root/insert_log/1/2/3", 2)
	writer := &growingBulkPackWriter{
		resolveManifestFormat: func(string, *indexpb.StorageConfig, []string, string) (string, error) {
			return "", nil
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		CollectionId:   1,
		PartitionId:    2,
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: manifestPath,
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}},
					},
				},
			},
		},
	}

	_, err := writer.currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrDataIntegrity))
}

func TestCurrentSplitForGrowingPackRejectsMalformedManifestPath(t *testing.T) {
	writer := &growingBulkPackWriter{
		resolveManifestFormat: func(string, *indexpb.StorageConfig, []string, string) (string, error) {
			t.Fatal("malformed manifest path must fail before reading object storage")
			return "", nil
		},
	}
	meta := &streamingpb.SegmentAssignmentMeta{
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: "not-json",
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}},
					},
				},
			},
		},
	}

	_, err := writer.currentSplitForGrowingPack(testGrowingPackSchema(), nil, meta)
	require.Error(t, err)
	require.True(t, errors.Is(err, merr.ErrDataIntegrity))
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
