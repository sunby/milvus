package idf

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newSegmentCacheAt(rootDir string) *segmentCache {
	return &segmentCache{
		rootDir: rootDir,
		entries: make(map[sealedCacheKey]*sealedBm25Stats),
	}
}

func testSealedCacheKey(t *testing.T, resource *datapb.StreamingNodeBM25Resource) sealedCacheKey {
	t.Helper()
	key, err := buildSealedCacheKey(resource)
	require.NoError(t, err)
	return key
}

func testLegacyBM25Resource(segmentID int64, statsPath string) *datapb.StreamingNodeBM25Resource {
	return &datapb.StreamingNodeBM25Resource{
		SegmentId:      segmentID,
		StorageVersion: storage.StorageV2,
		Bm25Binlogs: []*datapb.FieldBinlog{{
			FieldID: testBM25OutputFieldID,
			Binlogs: []*datapb.Binlog{{LogPath: statsPath}},
		}},
	}
}

func TestSegmentCacheUsesManifestPathAsStorageSourceOfTruth(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	statsPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{1: 2, 2: 1})

	manifestPath := packed.MarshalManifestPath("files/insert_log/1/2/3", 1)
	newResolver := mockey.Mock(packed.NewStatsResolver).To(func(path string, config *indexpb.StorageConfig) *packed.StatsResolver {
		require.Equal(t, manifestPath, path)
		require.NotNil(t, config)
		return &packed.StatsResolver{}
	}).Build()
	defer newResolver.UnPatch()
	resolvePaths := mockey.Mock((*packed.StatsResolver).BM25StatsPaths).Return(map[int64][]string{
		102: {statsPath},
	}, nil).Build()
	defer resolvePaths.UnPatch()

	cache := newSegmentCacheAt(t.TempDir())
	resource := &datapb.StreamingNodeBM25Resource{
		SegmentId:      3,
		StorageVersion: storage.StorageV2,
		ManifestPath:   manifestPath,
	}
	loaded, sealedStats, err := cache.acquire(ctx, chunkManager, resource, true)
	require.NoError(t, err)
	require.Equal(t, int64(1), loaded[102].NumRow())
	require.Equal(t, float64(3), loaded[102].GetAvgdl())

	entry := cache.entries[testSealedCacheKey(t, resource)]
	require.NotNil(t, entry)
	require.Same(t, sealedStats, entry)
	require.Nil(t, entry.load)
	require.FileExists(t, entry.localDir+"/102/0.data")

	fetched, err := sealedStats.FetchStats()
	require.NoError(t, err)
	require.Equal(t, int64(1), fetched[102].NumRow())
	localDir := entry.localDir
	cache.release(sealedStats)
	require.NoDirExists(t, localDir)
	require.Empty(t, cache.entries)
}

func TestSegmentCacheRejectsStorageV3WithoutManifest(t *testing.T) {
	paramtable.Init()
	cache := newSegmentCacheAt(t.TempDir())
	_, _, err := cache.acquire(context.Background(), storage.NewLocalChunkManager(), &datapb.StreamingNodeBM25Resource{
		SegmentId:      3,
		StorageVersion: storage.StorageV3,
	}, true)
	require.Error(t, err)
	require.Empty(t, cache.entries)
}

func TestSegmentCacheReusesLocalFilesWithoutRetainingStats(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	statsPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{1: 1})
	resource := testLegacyBM25Resource(3, statsPath)

	cache := newSegmentCacheAt(t.TempDir())
	first, firstSealedStats, err := cache.acquire(ctx, chunkManager, resource, true)
	require.NoError(t, err)
	require.NoError(t, os.Remove(statsPath))
	second, secondSealedStats, err := cache.acquire(ctx, chunkManager, proto.Clone(resource).(*datapb.StreamingNodeBM25Resource), true)
	require.NoError(t, err)
	require.Equal(t, first[102].NumRow(), second[102].NumRow())
	require.NotSame(t, first[102], second[102])

	entry := cache.entries[testSealedCacheKey(t, resource)]
	require.NotNil(t, entry)
	require.Same(t, firstSealedStats, entry)
	require.Same(t, secondSealedStats, entry)
	require.Equal(t, 2, entry.refs)
	localDir := entry.localDir

	cache.release(firstSealedStats)
	require.DirExists(t, localDir)
	cache.release(secondSealedStats)
	require.NoDirExists(t, localDir)
	require.Empty(t, cache.entries)
}

func TestSegmentCacheKeepsDifferentResourcesForSameSegmentSeparate(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	firstPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{1: 1})
	secondPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{1: 4})
	firstResource := testLegacyBM25Resource(3, firstPath)
	secondResource := testLegacyBM25Resource(3, secondPath)
	firstKey := testSealedCacheKey(t, firstResource)
	secondKey := testSealedCacheKey(t, secondResource)
	require.NotEqual(t, firstKey, secondKey)

	cache := newSegmentCacheAt(t.TempDir())
	first, firstEntry, err := cache.acquire(ctx, chunkManager, firstResource, true)
	require.NoError(t, err)
	second, secondEntry, err := cache.acquire(ctx, chunkManager, secondResource, true)
	require.NoError(t, err)
	require.NotSame(t, firstEntry, secondEntry)
	require.Equal(t, float64(1), first[102].GetAvgdl())
	require.Equal(t, float64(4), second[102].GetAvgdl())
	require.Len(t, cache.entries, 2)
	firstDir := firstEntry.localDir
	secondDir := secondEntry.localDir
	require.NotEqual(t, firstDir, secondDir)

	cache.release(firstEntry)
	require.NoDirExists(t, firstDir)
	require.DirExists(t, secondDir)
	require.Same(t, secondEntry, cache.entries[secondKey])
	fetched, err := secondEntry.FetchStats()
	require.NoError(t, err)
	require.Equal(t, float64(4), fetched[102].GetAvgdl())
	cache.release(secondEntry)
	require.NoDirExists(t, secondDir)
	require.Empty(t, cache.entries)
}

func TestSegmentCacheDiskOnlyAcquireCanFetchStatsLater(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	statsPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{1: 4})
	cache := newSegmentCacheAt(t.TempDir())

	loaded, sealedStats, err := cache.acquire(ctx, chunkManager, testLegacyBM25Resource(3, statsPath), false)
	require.NoError(t, err)
	require.Nil(t, loaded)

	fetched, err := sealedStats.FetchStats()
	require.NoError(t, err)
	require.Equal(t, int64(1), fetched[102].NumRow())
	require.Equal(t, float64(4), fetched[102].GetAvgdl())
	cache.release(sealedStats)
}

func TestSegmentCacheReportsCorruptDiskOnlyStats(t *testing.T) {
	ctx := context.Background()
	chunkManager := storage.NewLocalChunkManager()
	statsPath := writeTestBM25Stats(t, chunkManager, map[uint32]float32{1: 4})
	cache := newSegmentCacheAt(t.TempDir())
	resource := testLegacyBM25Resource(3, statsPath)

	_, sealedStats, err := cache.acquire(ctx, chunkManager, resource, false)
	require.NoError(t, err)
	entry := cache.entries[testSealedCacheKey(t, resource)]
	require.NotNil(t, entry)
	require.NoError(t, os.WriteFile(entry.localDir+"/102/0.data", []byte{1, 2, 3}, 0o600))

	_, err = sealedStats.FetchStats()
	require.ErrorIs(t, err, merr.ErrSerializationFailed)
	localDir := entry.localDir
	cache.release(sealedStats)
	require.NoDirExists(t, localDir)
}

func TestSegmentCacheCoalescesConcurrentLoads(t *testing.T) {
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 2})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)

	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, "stats-3").
		RunAndReturn(func(context.Context, string) (storage.FileReader, error) {
			close(readStarted)
			<-releaseRead
			return &testBytesFileReader{Reader: bytes.NewReader(statsBytes)}, nil
		}).Once()

	cache := newSegmentCacheAt(t.TempDir())
	resource := testLegacyBM25Resource(3, "stats-3")
	type result struct {
		stats       bm25Stats
		sealedStats *sealedBm25Stats
		err         error
	}
	results := make(chan result, 2)
	acquire := func() {
		loaded, sealedStats, err := cache.acquire(context.Background(), chunkManager, resource, true)
		results <- result{stats: loaded, sealedStats: sealedStats, err: err}
	}
	go acquire()
	<-readStarted
	go acquire()
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		for _, entry := range cache.entries {
			return entry.refs == 2
		}
		return false
	}, time.Second, time.Millisecond)
	close(releaseRead)

	for range 2 {
		result := <-results
		require.NoError(t, result.err)
		require.Equal(t, int64(1), result.stats[102].NumRow())
		cache.release(result.sealedStats)
	}
	require.Empty(t, cache.entries)
}

func TestSegmentCacheWaiterRetriesCanceledSharedLoad(t *testing.T) {
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 2})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)

	firstReadStarted := make(chan struct{})
	var calls atomic.Int32
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, "stats-3").
		RunAndReturn(func(ctx context.Context, _ string) (storage.FileReader, error) {
			if calls.Add(1) == 1 {
				close(firstReadStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &testBytesFileReader{Reader: bytes.NewReader(statsBytes)}, nil
		}).Twice()

	cache := newSegmentCacheAt(t.TempDir())
	resource := testLegacyBM25Resource(3, "stats-3")
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerResult := make(chan error, 1)
	go func() {
		_, _, err := cache.acquire(ownerCtx, chunkManager, resource, true)
		ownerResult <- err
	}()
	<-firstReadStarted

	type result struct {
		stats       bm25Stats
		sealedStats *sealedBm25Stats
		err         error
	}
	waiterResult := make(chan result, 1)
	go func() {
		loaded, sealedStats, err := cache.acquire(context.Background(), chunkManager, resource, true)
		waiterResult <- result{stats: loaded, sealedStats: sealedStats, err: err}
	}()
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		for _, entry := range cache.entries {
			return entry.refs == 2
		}
		return false
	}, time.Second, time.Millisecond)

	cancelOwner()
	require.ErrorIs(t, <-ownerResult, context.Canceled)
	waiter := <-waiterResult
	require.NoError(t, waiter.err)
	require.Equal(t, int64(1), waiter.stats[102].NumRow())
	cache.release(waiter.sealedStats)
	require.Equal(t, int32(2), calls.Load())
	require.Empty(t, cache.entries)
}

func TestSegmentCacheStaleEntryDoesNotReleaseReplacement(t *testing.T) {
	cache := newSegmentCacheAt(t.TempDir())
	key := sealedCacheKey("resource")
	stale := &sealedBm25Stats{key: key, segmentID: 3, refs: 1}
	replacement := &sealedBm25Stats{key: key, segmentID: 3, refs: 1}
	cache.entries[key] = replacement

	cache.release(stale)

	require.Same(t, replacement, cache.entries[key])
	require.Equal(t, 1, replacement.refs)
}

func TestSegmentCacheRemovesPartialFilesAfterDeserializeFailure(t *testing.T) {
	chunkManager := mocks.NewChunkManager(t)
	chunkManager.EXPECT().Reader(mock.Anything, "truncated-stats").
		Return(&testBytesFileReader{Reader: bytes.NewReader([]byte{1, 2, 3})}, nil).
		Once()

	rootDir := t.TempDir()
	cache := newSegmentCacheAt(rootDir)
	_, _, err := cache.acquire(context.Background(), chunkManager, testLegacyBM25Resource(3, "truncated-stats"), true)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrSerializationFailed)
	require.Empty(t, cache.entries)
	entries, readErr := os.ReadDir(rootDir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestSegmentCacheClassifiesRemoteOpenErrors(t *testing.T) {
	tests := []struct {
		name      string
		remoteErr error
		expected  error
	}{
		{
			name:      "preserve typed throttling",
			remoteErr: merr.WrapErrIoTooManyRequests("stats-3", merr.ErrIoFailed),
			expected:  merr.ErrIoTooManyRequests,
		},
		{
			name:      "classify raw IO failure",
			remoteErr: os.ErrPermission,
			expected:  merr.ErrIoFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunkManager := mocks.NewChunkManager(t)
			chunkManager.EXPECT().Reader(mock.Anything, "stats-3").Return(nil, test.remoteErr).Once()

			rootDir := t.TempDir()
			cache := newSegmentCacheAt(rootDir)
			_, _, err := cache.acquire(context.Background(), chunkManager, testLegacyBM25Resource(3, "stats-3"), false)
			require.ErrorIs(t, err, test.expected)
			require.Empty(t, cache.entries)
			entries, readErr := os.ReadDir(rootDir)
			require.NoError(t, readErr)
			require.Empty(t, entries)
		})
	}
}

func TestSegmentCacheClassifiesRemoteReadErrorsAndRemovesPartialFiles(t *testing.T) {
	stats := storage.NewBM25Stats()
	stats.Append(map[uint32]float32{1: 2})
	statsBytes, err := stats.Serialize()
	require.NoError(t, err)
	tests := []struct {
		name      string
		remoteErr error
		expected  error
		needParse bool
	}{
		{name: "throttled", remoteErr: minio.ErrorResponse{Code: "SlowDown", Message: "throttled"}, expected: merr.ErrIoTooManyRequests, needParse: true},
		{name: "unexpected EOF", remoteErr: io.ErrUnexpectedEOF, expected: merr.ErrIoUnexpectEOF, needParse: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			chunkManager := mocks.NewChunkManager(t)
			chunkManager.EXPECT().Reader(mock.Anything, "stats-3").Return(&testFailingFileReader{
				reader: bytes.NewReader(statsBytes[:20]),
				err:    test.remoteErr,
			}, nil).Once()

			rootDir := t.TempDir()
			cache := newSegmentCacheAt(rootDir)
			_, _, err := cache.acquire(context.Background(), chunkManager, testLegacyBM25Resource(3, "stats-3"), test.needParse)
			require.ErrorIs(t, err, test.expected)
			require.Empty(t, cache.entries)
			entries, readErr := os.ReadDir(rootDir)
			require.NoError(t, readErr)
			require.Empty(t, entries)
		})
	}
}

type testFailingFileReader struct {
	reader *bytes.Reader
	err    error
}

func (r *testFailingFileReader) Read(buffer []byte) (int, error) {
	if r.reader.Len() > 0 {
		return r.reader.Read(buffer)
	}
	return 0, r.err
}

func (r *testFailingFileReader) ReadAt(buffer []byte, offset int64) (int, error) {
	return r.reader.ReadAt(buffer, offset)
}

func (r *testFailingFileReader) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

func (*testFailingFileReader) Close() error           { return nil }
func (r *testFailingFileReader) Size() (int64, error) { return r.reader.Size(), nil }

func writeTestBM25Stats(t *testing.T, chunkManager storage.ChunkManager, row map[uint32]float32) string {
	t.Helper()
	stats := storage.NewBM25Stats()
	stats.Append(row)
	bytes, err := stats.Serialize()
	require.NoError(t, err)
	statsPath := t.TempDir() + "/bm25-stats"
	require.NoError(t, chunkManager.Write(context.Background(), statsPath, bytes))
	return statsPath
}
