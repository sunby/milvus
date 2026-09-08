package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadCollectionSyncWarmupOption(t *testing.T) {
	ordinary := NewLoadCollectionOption("items").Request()
	require.Empty(t, ordinary.GetLoadParams())
	option := NewLoadCollectionOption("items").WithSyncWarmup()
	request := option.Request()
	require.Equal(t, "sync", request.GetLoadParams()["warmup"])
	request.LoadParams["warmup"] = "disable"
	require.Equal(t, "sync", option.Request().GetLoadParams()["warmup"], "requests must not share mutable maps")
}
