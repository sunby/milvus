package sessionutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/json"
)

func TestSyncLoadWarmupCapabilityRoundTrip(t *testing.T) {
	session := &Session{}
	WithSyncLoadWarmup(true)(session)
	encoded, err := json.Marshal(session)
	require.NoError(t, err)
	var restored Session
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.True(t, restored.SyncLoadWarmup)
	var legacy Session
	require.NoError(t, json.Unmarshal([]byte(`{"ServerID":1}`), &legacy))
	require.False(t, legacy.SyncLoadWarmup, "unknown capability must fail closed")
}
