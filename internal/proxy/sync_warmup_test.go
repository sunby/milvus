package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadCollectionWarmupValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		params  map[string]string
		refresh bool
		valid   bool
	}{
		{"omitted", nil, false, true},
		{"sync", map[string]string{"warmup": "sync"}, false, true},
		{"empty", map[string]string{"warmup": ""}, false, false},
		{"disable", map[string]string{"warmup": "disable"}, false, false},
		{"async", map[string]string{"warmup": "async"}, false, false},
		{"refresh", map[string]string{"warmup": "sync"}, true, false},
		{"ordinary refresh", nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &loadCollectionTask{LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
				CollectionName: "warmup_test", LoadParams: tc.params, Refresh: tc.refresh,
			}}
			err := task.PreExecute(context.Background())
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			}
		})
	}
}
