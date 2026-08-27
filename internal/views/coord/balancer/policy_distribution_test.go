package balancer

import (
	"math"
	"math/bits"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
)

const runQVBalanceScaleTestEnv = "MILVUS_QV_BALANCE_SCALE_TEST"

type normalDistributionSimulationConfig struct {
	collectionCount         int
	segmentCount            int
	totalRows               int64
	maxCollectionRows       int64
	forcedMaxCollectionRows int64
	queryNodeCount          int
	shardsPerCollection     int
	segmentCountStdDev      float64
	collectionRowsStdDev    float64
	segmentRowCorrelation   float64
	segmentRowWeightStdDev  float64
	randomSeed              int64
	balanceConfig           *BalanceConfig
}

type syntheticDistributionShard struct {
	collectionIndex int32
	shardIndex      int32
	segmentOffset   int32
	segmentCount    int32
	totalRows       int64
}

type normalDistributionSimulationResult struct {
	assignedSegmentCount         int
	totalRows                    int64
	maxCollectionRows            int64
	largestCollectionNodeCount   int
	collectionsAboveFanoutTarget int
	segmentCountMean             float64
	segmentCountStdDev           float64
	minCollectionSegments        int
	maxCollectionSegments        int
	segmentCountSkewness         float64
	segmentCountExcessKurtosis   float64
	collectionRowsMean           float64
	collectionRowsStdDev         float64
	collectionRowsSkewness       float64
	collectionRowsExcessKurtosis float64
	segmentRowCorrelation        float64
	collocatedCollectionRate     float64
	singleNodeShardRate          float64
	meanNodesPerCollection       float64
	p95NodesPerCollection        int
	nodeRowCoefficientVariation  float64
	nodeRowMaxDeviation          float64
	nodeRows                     []int64
}

type sampleMoments struct {
	count int
	sum   float64
	sumSq float64
}

func (m *sampleMoments) add(value float64) {
	m.count++
	m.sum += value
	m.sumSq += value * value
}

func (m sampleMoments) mean() float64 {
	if m.count == 0 {
		return 0
	}
	return m.sum / float64(m.count)
}

func (m sampleMoments) stdDev() float64 {
	if m.count == 0 {
		return 0
	}
	mean := m.mean()
	variance := m.sumSq/float64(m.count) - mean*mean
	if variance < 0 {
		variance = 0
	}
	return math.Sqrt(variance)
}

func TestDefaultBalancePolicy_CorrelatedNormalDistribution(t *testing.T) {
	cfg := normalDistributionSimulationConfig{
		collectionCount:        10_000,
		segmentCount:           100_000,
		totalRows:              50_000_000,
		maxCollectionRows:      500_000_000,
		queryNodeCount:         8,
		shardsPerCollection:    1,
		segmentCountStdDev:     3,
		collectionRowsStdDev:   1_500,
		segmentRowCorrelation:  0.9,
		segmentRowWeightStdDev: 0.25,
		randomSeed:             20260827,
	}

	result := simulateNormalDistributionPlacement(t, cfg)
	logNormalDistributionResult(t, cfg, result)

	assert.Equal(t, cfg.segmentCount, result.assignedSegmentCount)
	assert.Equal(t, cfg.totalRows, result.totalRows)
	assert.LessOrEqual(t, result.maxCollectionRows, cfg.maxCollectionRows)
	assert.InDelta(t, cfg.segmentRowCorrelation, result.segmentRowCorrelation, 0.02)
	assert.Less(t, math.Abs(result.segmentCountSkewness), 0.10)
	assert.Less(t, math.Abs(result.segmentCountExcessKurtosis), 0.15)
	assert.Less(t, math.Abs(result.collectionRowsSkewness), 0.10)
	assert.Less(t, math.Abs(result.collectionRowsExcessKurtosis), 0.15)
	assert.Zero(t, result.collectionsAboveFanoutTarget)
	assert.Equal(t, 1.0, result.collocatedCollectionRate)
	assert.InDelta(t, result.collocatedCollectionRate, result.singleNodeShardRate, 0)
	assert.Less(t, result.nodeRowCoefficientVariation, 0.001)
	assert.Less(t, result.nodeRowMaxDeviation, 0.002)
}

// TestDefaultBalancePolicy_MillionCollectionsTenMillionSegments is opt-in
// because it intentionally generates and scores exactly ten million segments.
//
// Assumptions isolate the one-collection-one-shard case: all collections use one
// replica and one Resource Group containing eight homogeneous QueryNodes; every
// collection has exactly one vchannel; segment count is N(10, 3) adjusted to an
// exact global total of ten million. Collection RowNum is generated as a
// correlated normal variable, then adjusted to an exact global total of five
// billion rows while preserving the requested correlation and 500-million cap.
// The test reuses the production Phase-2 scorer but does not materialize one
// million LoadConfigs, protobuf DataViews, or QueryView builders, so it is not
// a control-plane memory, reconciliation-latency, or end-to-end scale test.
func TestDefaultBalancePolicy_MillionCollectionsTenMillionSegments(t *testing.T) {
	if os.Getenv(runQVBalanceScaleTestEnv) != "1" {
		t.Skipf("set %s=1 to run the million-collection distribution test", runQVBalanceScaleTestEnv)
	}

	cfg := normalDistributionSimulationConfig{
		collectionCount:        1_000_000,
		segmentCount:           10_000_000,
		totalRows:              5_000_000_000,
		maxCollectionRows:      500_000_000,
		queryNodeCount:         8,
		shardsPerCollection:    1,
		segmentCountStdDev:     3,
		collectionRowsStdDev:   1_500,
		segmentRowCorrelation:  0.9,
		segmentRowWeightStdDev: 0.25,
		randomSeed:             20260827,
	}

	started := time.Now()
	result := simulateNormalDistributionPlacement(t, cfg)
	logNormalDistributionResult(t, cfg, result)
	t.Logf("simulation wall time: %s", time.Since(started))

	assert.Equal(t, cfg.segmentCount, result.assignedSegmentCount)
	assert.Equal(t, cfg.totalRows, result.totalRows)
	assert.LessOrEqual(t, result.maxCollectionRows, cfg.maxCollectionRows)
	assert.InDelta(t, cfg.segmentRowCorrelation, result.segmentRowCorrelation, 0.01)
	assert.Less(t, math.Abs(result.segmentCountSkewness), 0.02)
	assert.Less(t, math.Abs(result.segmentCountExcessKurtosis), 0.05)
	assert.Less(t, math.Abs(result.collectionRowsSkewness), 0.02)
	assert.Less(t, math.Abs(result.collectionRowsExcessKurtosis), 0.05)
	assert.Zero(t, result.collectionsAboveFanoutTarget)
	assert.Equal(t, 1.0, result.collocatedCollectionRate)
	assert.InDelta(t, result.collocatedCollectionRate, result.singleNodeShardRate, 0)
	assert.Less(t, result.nodeRowCoefficientVariation, 0.0001)
	assert.Less(t, result.nodeRowMaxDeviation, 0.0002)
}

func TestDefaultBalancePolicy_ParameterABWith500MCollection(t *testing.T) {
	if os.Getenv(runQVBalanceScaleTestEnv) != "1" {
		t.Skipf("set %s=1 to run the million-collection parameter A/B test", runQVBalanceScaleTestEnv)
	}

	base := normalDistributionSimulationConfig{
		collectionCount:         1_000_000,
		segmentCount:            10_000_000,
		totalRows:               5_000_000_000,
		maxCollectionRows:       500_000_000,
		forcedMaxCollectionRows: 500_000_000,
		queryNodeCount:          8,
		shardsPerCollection:     1,
		segmentCountStdDev:      3,
		collectionRowsStdDev:    1_500,
		segmentRowCorrelation:   0.9,
		segmentRowWeightStdDev:  0.25,
		randomSeed:              20260827,
	}

	defaultConfig := DefaultBalanceConfig()
	targetOnlyConfig := DefaultBalanceConfig()
	targetOnlyConfig.TargetRowsPerShardNode = 500_000_000
	strongFanoutConfig := DefaultBalanceConfig()
	strongFanoutConfig.TargetRowsPerShardNode = 500_000_000
	strongFanoutConfig.FanoutWeight = 3

	results := make(map[string]normalDistributionSimulationResult)
	for _, testCase := range []struct {
		name   string
		config *BalanceConfig
	}{
		{name: "default", config: defaultConfig},
		{name: "target_500m", config: targetOnlyConfig},
		{name: "target_500m_fanout_weight_3", config: strongFanoutConfig},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := base
			cfg.balanceConfig = testCase.config
			result := simulateNormalDistributionPlacement(t, cfg)
			results[testCase.name] = result
			logNormalDistributionResult(t, cfg, result)

			assert.Equal(t, cfg.segmentCount, result.assignedSegmentCount)
			assert.Equal(t, cfg.totalRows, result.totalRows)
			assert.Equal(t, cfg.forcedMaxCollectionRows, result.maxCollectionRows)
			assert.Less(t, result.nodeRowMaxDeviation, 0.001)
		})
	}

	assert.Equal(t, 8, results["default"].largestCollectionNodeCount)
	assert.Equal(t, 1, results["target_500m"].largestCollectionNodeCount)
	assert.Equal(t, 1, results["target_500m_fanout_weight_3"].largestCollectionNodeCount)
	assert.Less(t, results["default"].collocatedCollectionRate, 1.0)
	assert.Equal(t, 1.0, results["target_500m"].collocatedCollectionRate)
	assert.Equal(t, 1.0, results["target_500m_fanout_weight_3"].collocatedCollectionRate)
}

func TestFanoutWeightConsolidatesExistingSpreadShard(t *testing.T) {
	nodes := map[int64]*BalanceNode{
		1: {NodeID: 1, Alive: true, ResourceGroup: "rg1"},
		2: {NodeID: 2, Alive: true, ResourceGroup: "rg1"},
	}
	currentStates := []map[int64]coordview.SegmentState{
		{1: coordview.SegmentStateUp},
		{2: coordview.SegmentStateUp},
	}

	allocateNodes := func(cfg *BalanceConfig) []int64 {
		ctx := newAllocationContext(nodes, "rg1", map[int64]int64{}, 2_000_000, 2, cfg)
		assigned := make([]int64, 0, len(currentStates))
		for _, states := range currentStates {
			nodeID, ok := pickNode(ctx, &SegmentInfo{RowNum: 1_000_000}, states)
			require.True(t, ok)
			assigned = append(assigned, nodeID)
			ctx.assign(nodeID, 1_000_000)
		}
		return assigned
	}

	defaultConfig := DefaultBalanceConfig()
	defaultConfig.TargetRowsPerShardNode = 500_000_000
	assert.Equal(t, []int64{1, 2}, allocateNodes(defaultConfig))

	strongFanoutConfig := DefaultBalanceConfig()
	strongFanoutConfig.TargetRowsPerShardNode = 500_000_000
	strongFanoutConfig.FanoutWeight = 3
	assert.Equal(t, []int64{1, 1}, allocateNodes(strongFanoutConfig))
}

func simulateNormalDistributionPlacement(
	t testing.TB,
	cfg normalDistributionSimulationConfig,
) normalDistributionSimulationResult {
	t.Helper()
	require.Greater(t, cfg.collectionCount, 0)
	require.GreaterOrEqual(t, cfg.segmentCount, cfg.collectionCount*cfg.shardsPerCollection)
	require.Greater(t, cfg.queryNodeCount, 0)
	require.LessOrEqual(t, cfg.queryNodeCount, 64)
	require.Greater(t, cfg.shardsPerCollection, 0)
	require.GreaterOrEqual(t, cfg.totalRows, int64(cfg.segmentCount))
	require.Greater(t, cfg.maxCollectionRows, int64(0))
	require.GreaterOrEqual(t, cfg.forcedMaxCollectionRows, int64(0))
	require.LessOrEqual(t, cfg.forcedMaxCollectionRows, cfg.maxCollectionRows)
	require.LessOrEqual(t, cfg.forcedMaxCollectionRows, cfg.totalRows)
	require.GreaterOrEqual(t, cfg.collectionRowsStdDev, 0.0)
	require.GreaterOrEqual(t, cfg.segmentRowCorrelation, -1.0)
	require.LessOrEqual(t, cfg.segmentRowCorrelation, 1.0)
	require.GreaterOrEqual(t, cfg.segmentRowWeightStdDev, 0.0)

	rng := rand.New(rand.NewSource(cfg.randomSeed))
	segmentCounts := normallyDistributedSegmentCounts(rng, cfg)
	collectionRowsByCollection := correlatedNormalCollectionRows(t, rng, cfg, segmentCounts)
	validateCorrelatedNormalDistribution(t, cfg, segmentCounts, collectionRowsByCollection)
	segmentRows := make([]int64, 0, cfg.segmentCount)
	shards := make([]syntheticDistributionShard, 0, cfg.collectionCount*cfg.shardsPerCollection)
	segmentCountMoments := sampleMoments{}
	collectionRowsMoments := sampleMoments{}
	balanceConfig := cfg.balanceConfig
	if balanceConfig == nil {
		balanceConfig = DefaultBalanceConfig()
	}
	fanoutTargetRows := balanceConfig.TargetRowsPerShardNode
	var (
		totalRows                    int64
		maxCollectionRows            int64
		collectionsAboveFanoutTarget int
		minCollectionSegments        = cfg.segmentCount
		maxCollectionSegments        int
		largestCollectionIndex       int
	)

	for collectionIndex, segmentCount := range segmentCounts {
		minCollectionSegments = min(minCollectionSegments, int(segmentCount))
		maxCollectionSegments = max(maxCollectionSegments, int(segmentCount))
		segmentCountMoments.add(float64(segmentCount))
		collectionRows := collectionRowsByCollection[collectionIndex]
		totalRows += collectionRows
		if collectionRows > maxCollectionRows {
			maxCollectionRows = collectionRows
			largestCollectionIndex = collectionIndex
		}
		if collectionRows > fanoutTargetRows {
			collectionsAboveFanoutTarget++
		}
		collectionRowsMoments.add(float64(collectionRows))

		rowsForCollection := distributeCollectionRows(rng, int(segmentCount), collectionRows, cfg.segmentRowWeightStdDev)
		collectionOffset := 0
		for shardIndex := 0; shardIndex < cfg.shardsPerCollection; shardIndex++ {
			count := int(segmentCount) / cfg.shardsPerCollection
			if shardIndex < int(segmentCount)%cfg.shardsPerCollection {
				count++
			}
			start := len(segmentRows)
			segmentRows = append(segmentRows, rowsForCollection[collectionOffset:collectionOffset+count]...)
			shardRows := segmentRows[start:]
			sort.Slice(shardRows, func(i, j int) bool {
				return shardRows[i] > shardRows[j]
			})
			var totalRows int64
			for _, rows := range shardRows {
				totalRows += rows
			}
			shards = append(shards, syntheticDistributionShard{
				collectionIndex: int32(collectionIndex),
				shardIndex:      int32(shardIndex),
				segmentOffset:   int32(start),
				segmentCount:    int32(count),
				totalRows:       totalRows,
			})
			collectionOffset += count
		}
	}
	require.Len(t, segmentRows, cfg.segmentCount)

	// DefaultBalancePolicy sorts mandatory candidates by shard RowNum before
	// allocation. These deterministic tie-breakers mirror shardLess for the
	// synthetic collection/vchannel identifiers.
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].totalRows != shards[j].totalRows {
			return shards[i].totalRows > shards[j].totalRows
		}
		if shards[i].collectionIndex != shards[j].collectionIndex {
			return shards[i].collectionIndex < shards[j].collectionIndex
		}
		return shards[i].shardIndex < shards[j].shardIndex
	})

	nodes := make(map[int64]*BalanceNode, cfg.queryNodeCount)
	nodeRows := make(map[int64]int64, cfg.queryNodeCount)
	eligible := make([]int64, 0, cfg.queryNodeCount)
	eligibleSet := make(map[int64]struct{}, cfg.queryNodeCount)
	for i := 0; i < cfg.queryNodeCount; i++ {
		nodeID := int64(i + 1)
		nodes[nodeID] = &BalanceNode{NodeID: nodeID, Alive: true, ResourceGroup: "rg1"}
		eligible = append(eligible, nodeID)
		eligibleSet[nodeID] = struct{}{}
	}
	allocation := &allocationContext{
		nodes:        nodes,
		eligible:     eligible,
		eligibleSet:  eligibleSet,
		baseRows:     make(map[int64]int64, cfg.queryNodeCount),
		assignedRows: make(map[int64]int64, cfg.queryNodeCount),
		openedNodes:  make(map[int64]struct{}, cfg.queryNodeCount),
		config:       balanceConfig,
	}
	collectionNodeMasks := make([]uint64, cfg.collectionCount)
	singleNodeShards := 0
	assignedSegments := 0
	segment := &SegmentInfo{}

	for _, shard := range shards {
		clear(allocation.assignedRows)
		clear(allocation.openedNodes)
		var totalBaseRows int64
		for _, nodeID := range allocation.eligible {
			allocation.baseRows[nodeID] = nodeRows[nodeID]
			totalBaseRows += nodeRows[nodeID]
		}
		allocation.referenceRows = float64(totalBaseRows+shard.totalRows) / float64(len(allocation.eligible))
		allocation.fanoutBudget = calculateFanoutBudget(
			len(allocation.eligible),
			int(shard.segmentCount),
			shard.totalRows,
			balanceConfig.TargetRowsPerShardNode,
		)

		var shardNodeMask uint64
		start := int(shard.segmentOffset)
		end := start + int(shard.segmentCount)
		for _, rows := range segmentRows[start:end] {
			segment.RowNum = rows
			nodeID, ok := pickNode(allocation, segment, nil)
			if !ok {
				t.Fatalf("no eligible node for synthetic shard %+v", shard)
			}
			allocation.assign(nodeID, rows)
			mask := uint64(1) << uint(nodeID-1)
			shardNodeMask |= mask
			collectionNodeMasks[int(shard.collectionIndex)] |= mask
			assignedSegments++
		}
		if bits.OnesCount64(shardNodeMask) == 1 {
			singleNodeShards++
		}
		for nodeID, rows := range allocation.assignedRows {
			nodeRows[nodeID] += rows
		}
	}

	nodesPerCollectionHistogram := make([]int, cfg.queryNodeCount+1)
	totalNodesPerCollection := 0
	for _, mask := range collectionNodeMasks {
		count := bits.OnesCount64(mask)
		nodesPerCollectionHistogram[count]++
		totalNodesPerCollection += count
	}

	nodeRowMoments := sampleMoments{}
	nodeRowValues := make([]int64, 0, cfg.queryNodeCount)
	for _, nodeID := range eligible {
		rows := nodeRows[nodeID]
		nodeRowValues = append(nodeRowValues, rows)
		nodeRowMoments.add(float64(rows))
	}
	nodeRowMean := nodeRowMoments.mean()
	maxDeviation := 0.0
	for _, rows := range nodeRowValues {
		deviation := math.Abs(float64(rows)-nodeRowMean) / nodeRowMean
		maxDeviation = math.Max(maxDeviation, deviation)
	}

	return normalDistributionSimulationResult{
		assignedSegmentCount:         assignedSegments,
		totalRows:                    totalRows,
		maxCollectionRows:            maxCollectionRows,
		largestCollectionNodeCount:   bits.OnesCount64(collectionNodeMasks[largestCollectionIndex]),
		collectionsAboveFanoutTarget: collectionsAboveFanoutTarget,
		segmentCountMean:             segmentCountMoments.mean(),
		segmentCountStdDev:           segmentCountMoments.stdDev(),
		minCollectionSegments:        minCollectionSegments,
		maxCollectionSegments:        maxCollectionSegments,
		segmentCountSkewness:         distributionSkewness(len(segmentCounts), segmentCountMoments.mean(), segmentCountMoments.stdDev(), func(i int) float64 { return float64(segmentCounts[i]) }),
		segmentCountExcessKurtosis:   distributionExcessKurtosis(len(segmentCounts), segmentCountMoments.mean(), segmentCountMoments.stdDev(), func(i int) float64 { return float64(segmentCounts[i]) }),
		collectionRowsMean:           collectionRowsMoments.mean(),
		collectionRowsStdDev:         collectionRowsMoments.stdDev(),
		collectionRowsSkewness:       distributionSkewness(len(collectionRowsByCollection), collectionRowsMoments.mean(), collectionRowsMoments.stdDev(), func(i int) float64 { return float64(collectionRowsByCollection[i]) }),
		collectionRowsExcessKurtosis: distributionExcessKurtosis(len(collectionRowsByCollection), collectionRowsMoments.mean(), collectionRowsMoments.stdDev(), func(i int) float64 { return float64(collectionRowsByCollection[i]) }),
		segmentRowCorrelation:        pearsonSegmentRowCorrelation(segmentCounts, collectionRowsByCollection),
		collocatedCollectionRate:     float64(nodesPerCollectionHistogram[1]) / float64(cfg.collectionCount),
		singleNodeShardRate:          float64(singleNodeShards) / float64(len(shards)),
		meanNodesPerCollection:       float64(totalNodesPerCollection) / float64(cfg.collectionCount),
		p95NodesPerCollection:        histogramPercentile(nodesPerCollectionHistogram, cfg.collectionCount, 0.95),
		nodeRowCoefficientVariation:  nodeRowMoments.stdDev() / nodeRowMean,
		nodeRowMaxDeviation:          maxDeviation,
		nodeRows:                     nodeRowValues,
	}
}

func normallyDistributedSegmentCounts(
	rng *rand.Rand,
	cfg normalDistributionSimulationConfig,
) []int32 {
	minimum := cfg.shardsPerCollection
	mean := float64(cfg.segmentCount) / float64(cfg.collectionCount)
	counts := make([]int32, cfg.collectionCount)
	total := 0
	for i := range counts {
		count := int(math.Round(mean + rng.NormFloat64()*cfg.segmentCountStdDev))
		if count < minimum {
			count = minimum
		}
		counts[i] = int32(count)
		total += count
	}

	for total < cfg.segmentCount {
		index := rng.Intn(len(counts))
		counts[index]++
		total++
	}
	for total > cfg.segmentCount {
		index := rng.Intn(len(counts))
		if int(counts[index]) == minimum {
			continue
		}
		counts[index]--
		total--
	}
	return counts
}

func correlatedNormalCollectionRows(
	t testing.TB,
	rng *rand.Rand,
	cfg normalDistributionSimulationConfig,
	segmentCounts []int32,
) []int64 {
	t.Helper()
	segmentMoments := sampleMoments{}
	for _, count := range segmentCounts {
		segmentMoments.add(float64(count))
	}
	segmentMean := segmentMoments.mean()
	segmentStdDev := segmentMoments.stdDev()
	rowMean := float64(cfg.totalRows) / float64(cfg.collectionCount)
	residualScale := math.Sqrt(math.Max(0, 1-cfg.segmentRowCorrelation*cfg.segmentRowCorrelation))

	rows := make([]int64, len(segmentCounts))
	for i, segmentCount := range segmentCounts {
		minimumRows := int64(segmentCount)
		if minimumRows > cfg.maxCollectionRows {
			t.Fatalf("collection %d needs at least %d rows for its segments, above cap %d", i, minimumRows, cfg.maxCollectionRows)
		}
		standardizedSegmentCount := 0.0
		if segmentStdDev > 0 {
			standardizedSegmentCount = (float64(segmentCount) - segmentMean) / segmentStdDev
		}
		standardizedRows := cfg.segmentRowCorrelation*standardizedSegmentCount + residualScale*rng.NormFloat64()
		rowCount := int64(math.Round(rowMean + cfg.collectionRowsStdDev*standardizedRows))
		rowCount = max(rowCount, minimumRows)
		rowCount = min(rowCount, cfg.maxCollectionRows)
		rows[i] = rowCount
	}

	pinnedIndex := -1
	if cfg.forcedMaxCollectionRows > 0 {
		pinnedIndex = 0
		for i := 1; i < len(segmentCounts); i++ {
			if segmentCounts[i] > segmentCounts[pinnedIndex] {
				pinnedIndex = i
			}
		}
		rows[pinnedIndex] = cfg.forcedMaxCollectionRows
	}

	adjustCollectionRowsToTotal(t, rows, segmentCounts, cfg.totalRows, cfg.maxCollectionRows, pinnedIndex)
	return rows
}

// validateCorrelatedNormalDistribution is the pre-balance gate. Placement is
// not simulated unless both marginals have the requested shape, both global
// totals are exact, and collections with more rows also have more segments.
func validateCorrelatedNormalDistribution(
	t testing.TB,
	cfg normalDistributionSimulationConfig,
	segmentCounts []int32,
	collectionRows []int64,
) {
	t.Helper()
	require.Len(t, segmentCounts, cfg.collectionCount)
	require.Len(t, collectionRows, cfg.collectionCount)

	segmentMoments := sampleMoments{}
	rowMoments := sampleMoments{}
	var segmentTotal int
	var rowTotal, maxRows int64
	for i := range segmentCounts {
		segmentTotal += int(segmentCounts[i])
		rowTotal += collectionRows[i]
		maxRows = max(maxRows, collectionRows[i])
		segmentMoments.add(float64(segmentCounts[i]))
		rowMoments.add(float64(collectionRows[i]))
	}

	segmentSkewness := distributionSkewness(len(segmentCounts), segmentMoments.mean(), segmentMoments.stdDev(), func(i int) float64 {
		return float64(segmentCounts[i])
	})
	segmentExcessKurtosis := distributionExcessKurtosis(len(segmentCounts), segmentMoments.mean(), segmentMoments.stdDev(), func(i int) float64 {
		return float64(segmentCounts[i])
	})
	rowSkewness := distributionSkewness(len(collectionRows), rowMoments.mean(), rowMoments.stdDev(), func(i int) float64 {
		return float64(collectionRows[i])
	})
	rowExcessKurtosis := distributionExcessKurtosis(len(collectionRows), rowMoments.mean(), rowMoments.stdDev(), func(i int) float64 {
		return float64(collectionRows[i])
	})

	require.Equal(t, cfg.segmentCount, segmentTotal)
	require.Equal(t, cfg.totalRows, rowTotal)
	require.LessOrEqual(t, maxRows, cfg.maxCollectionRows)
	require.Less(t, math.Abs(segmentSkewness), 0.10)
	require.Less(t, math.Abs(segmentExcessKurtosis), 0.15)
	if cfg.forcedMaxCollectionRows == 0 {
		require.InDelta(t, cfg.segmentRowCorrelation, pearsonSegmentRowCorrelation(segmentCounts, collectionRows), 0.02)
		require.Less(t, math.Abs(rowSkewness), 0.10)
		require.Less(t, math.Abs(rowExcessKurtosis), 0.15)
		return
	}

	require.Equal(t, cfg.forcedMaxCollectionRows, maxRows)
	maxSegmentCount := int32(0)
	forcedSegmentCount := int32(0)
	for i, rowCount := range collectionRows {
		maxSegmentCount = max(maxSegmentCount, segmentCounts[i])
		if rowCount == cfg.forcedMaxCollectionRows {
			forcedSegmentCount = segmentCounts[i]
		}
	}
	require.Equal(t, maxSegmentCount, forcedSegmentCount)
}

func adjustCollectionRowsToTotal(
	t testing.TB,
	rows []int64,
	segmentCounts []int32,
	targetRows int64,
	maxCollectionRows int64,
	pinnedIndex int,
) {
	t.Helper()
	var totalRows int64
	for _, rowCount := range rows {
		totalRows += rowCount
	}

	for totalRows != targetRows {
		if totalRows < targetRows {
			remaining := targetRows - totalRows
			adjustable := 0
			for i, rowCount := range rows {
				if i == pinnedIndex {
					continue
				}
				if rowCount < maxCollectionRows {
					adjustable++
				}
			}
			if adjustable == 0 {
				t.Fatalf("cannot add %d rows without exceeding the per-collection cap", remaining)
			}
			share := (remaining + int64(adjustable) - 1) / int64(adjustable)
			for i := range rows {
				if i == pinnedIndex {
					continue
				}
				capacity := maxCollectionRows - rows[i]
				if capacity <= 0 {
					continue
				}
				add := min(remaining, min(share, capacity))
				rows[i] += add
				remaining -= add
				totalRows += add
				if remaining == 0 {
					break
				}
			}
			continue
		}

		remaining := totalRows - targetRows
		adjustable := 0
		for i, rowCount := range rows {
			if i == pinnedIndex {
				continue
			}
			if rowCount > int64(segmentCounts[i]) {
				adjustable++
			}
		}
		if adjustable == 0 {
			t.Fatalf("cannot remove %d rows without making a segment empty", remaining)
		}
		share := (remaining + int64(adjustable) - 1) / int64(adjustable)
		for i := range rows {
			if i == pinnedIndex {
				continue
			}
			capacity := rows[i] - int64(segmentCounts[i])
			if capacity <= 0 {
				continue
			}
			remove := min(remaining, min(share, capacity))
			rows[i] -= remove
			remaining -= remove
			totalRows -= remove
			if remaining == 0 {
				break
			}
		}
	}
}

func pearsonSegmentRowCorrelation(segmentCounts []int32, collectionRows []int64) float64 {
	if len(segmentCounts) == 0 || len(segmentCounts) != len(collectionRows) {
		return 0
	}
	var segmentSum, rowSum float64
	for i := range segmentCounts {
		segmentSum += float64(segmentCounts[i])
		rowSum += float64(collectionRows[i])
	}
	segmentMean := segmentSum / float64(len(segmentCounts))
	rowMean := rowSum / float64(len(collectionRows))
	var covariance, segmentVariance, rowVariance float64
	for i := range segmentCounts {
		segmentDelta := float64(segmentCounts[i]) - segmentMean
		rowDelta := float64(collectionRows[i]) - rowMean
		covariance += segmentDelta * rowDelta
		segmentVariance += segmentDelta * segmentDelta
		rowVariance += rowDelta * rowDelta
	}
	if segmentVariance == 0 || rowVariance == 0 {
		return 0
	}
	return covariance / math.Sqrt(segmentVariance*rowVariance)
}

func distributionSkewness(count int, mean, stdDev float64, valueAt func(int) float64) float64 {
	if count == 0 || stdDev == 0 {
		return 0
	}
	var thirdMoment float64
	for i := 0; i < count; i++ {
		standardized := (valueAt(i) - mean) / stdDev
		thirdMoment += standardized * standardized * standardized
	}
	return thirdMoment / float64(count)
}

func distributionExcessKurtosis(count int, mean, stdDev float64, valueAt func(int) float64) float64 {
	if count == 0 || stdDev == 0 {
		return 0
	}
	var fourthMoment float64
	for i := 0; i < count; i++ {
		standardized := (valueAt(i) - mean) / stdDev
		fourthMoment += standardized * standardized * standardized * standardized
	}
	return fourthMoment/float64(count) - 3
}

func distributeCollectionRows(
	rng *rand.Rand,
	segmentCount int,
	totalRows int64,
	weightStdDev float64,
) []int64 {
	weights := make([]float64, segmentCount)
	var weightTotal float64
	for i := range weights {
		weight := 1 + rng.NormFloat64()*weightStdDev
		if weight < 0.01 {
			weight = 0.01
		}
		weights[i] = weight
		weightTotal += weight
	}

	rows := make([]int64, segmentCount)
	extras := totalRows - int64(segmentCount)
	var assigned int64
	for i, weight := range weights {
		rows[i] = 1 + int64(math.Floor(float64(extras)*weight/weightTotal))
		assigned += rows[i]
	}
	for i := 0; assigned < totalRows; i = (i + 1) % len(rows) {
		rows[i]++
		assigned++
	}
	return rows
}

func histogramPercentile(histogram []int, total int, percentile float64) int {
	target := int(math.Ceil(float64(total) * percentile))
	seen := 0
	for value, count := range histogram {
		seen += count
		if seen >= target {
			return value
		}
	}
	return len(histogram) - 1
}

func logNormalDistributionResult(
	t testing.TB,
	cfg normalDistributionSimulationConfig,
	result normalDistributionSimulationResult,
) {
	t.Helper()
	balanceConfig := cfg.balanceConfig
	if balanceConfig == nil {
		balanceConfig = DefaultBalanceConfig()
	}
	t.Logf(
		"collections=%d segments=%d rows=%d max-collection-rows=%d cap=%d forced-max=%d qn=%d shards/collection=%d; "+
			"segment-count mean/stddev/min/max/skew/excess-kurtosis=%.2f/%.2f/%d/%d/%.4f/%.4f; "+
			"collection-rows mean/stddev/skew/excess-kurtosis=%.0f/%.0f/%.4f/%.4f; "+
			"segment-row correlation=%.6f collections-above-fanout-target=%d; "+
			"target-rows=%d weights(sticky/load/fanout)=%.1f/%.1f/%.1f; "+
			"collocated collections=%.4f%% single-node shards=%.4f%% largest-collection-nodes=%d mean/p95 nodes per collection=%.4f/%d; "+
			"node-row CV=%.6f%% max deviation=%.6f%% rows=%v",
		cfg.collectionCount,
		cfg.segmentCount,
		result.totalRows,
		result.maxCollectionRows,
		cfg.maxCollectionRows,
		cfg.forcedMaxCollectionRows,
		cfg.queryNodeCount,
		cfg.shardsPerCollection,
		result.segmentCountMean,
		result.segmentCountStdDev,
		result.minCollectionSegments,
		result.maxCollectionSegments,
		result.segmentCountSkewness,
		result.segmentCountExcessKurtosis,
		result.collectionRowsMean,
		result.collectionRowsStdDev,
		result.collectionRowsSkewness,
		result.collectionRowsExcessKurtosis,
		result.segmentRowCorrelation,
		result.collectionsAboveFanoutTarget,
		balanceConfig.TargetRowsPerShardNode,
		balanceConfig.StickinessWeight,
		balanceConfig.NodeLoadWeight,
		balanceConfig.FanoutWeight,
		result.collocatedCollectionRate*100,
		result.singleNodeShardRate*100,
		result.largestCollectionNodeCount,
		result.meanNodesPerCollection,
		result.p95NodesPerCollection,
		result.nodeRowCoefficientVariation*100,
		result.nodeRowMaxDeviation*100,
		result.nodeRows,
	)
}
