package paramtable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadCollectionSyncWarmupGateDefaultsClosed(t *testing.T) {
	base := NewBaseTable(Files([]string{}), SkipRemote(true), SkipEnv(true))
	var cfg queryCoordConfig
	cfg.init(base)
	require.Equal(t, "queryCoord.enableLoadCollectionSyncWarmup", cfg.EnableLoadCollectionSyncWarmup.Key)
	require.False(t, cfg.EnableLoadCollectionSyncWarmup.GetAsBool())
	require.NoError(t, base.Save(cfg.EnableLoadCollectionSyncWarmup.Key, "true"))
	require.True(t, cfg.EnableLoadCollectionSyncWarmup.GetAsBool())
}
