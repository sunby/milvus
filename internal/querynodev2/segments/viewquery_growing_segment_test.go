//go:build test && dynamic

package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
)

func TestViewQueryGrowingSegmentBatchPkExistIsConservative(t *testing.T) {
	segment := NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{CollectionID: 10}, nil)
	assert.NoError(t, segment.Prewarm(context.Background(), []int64{100}))

	hits := segment.BatchPkExist(storage.NewBatchLocationsCache([]storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(2),
	}))

	assert.Equal(t, []bool{true, true}, hits)
}
