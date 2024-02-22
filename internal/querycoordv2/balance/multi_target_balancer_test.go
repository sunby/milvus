package balance

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type MultiTargetBalancerTestSuite struct {
	suite.Suite
}

func (suite *MultiTargetBalancerTestSuite) SetupSuite() {
	paramtable.Init()
	rand.Seed(time.Now().UnixNano())
}

func (suite *MultiTargetBalancerTestSuite) TestRowCountCostModel() {
	cases := [][]struct {
		nodeID    int64
		segmentID int64
		rowCount  int64
	}{
		// case 1, empty cluster
		{},
		// case 2
		// node 0: 30, node 1: 0
		{{0, 1, 30}, {1, 0, 0}},
		// case 3
		// node 0: 30, node 1: 30
		{{0, 1, 30}, {1, 2, 30}},
		// case 4
		// node 0: 30, node 1: 20, node 2: 10
		{{0, 1, 30}, {1, 2, 20}, {2, 3, 10}},
		// case 5
		{{0, 1, 30}},
	}

	expects := []float64{
		0,
		1,
		0,
		0.25,
		0,
	}

	for i, c := range cases {
		nodeSegments := make(map[int64][]*meta.Segment)
		for _, v := range c {
			nodeSegments[v.nodeID] = append(nodeSegments[v.nodeID],
				&meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: v.segmentID, NumOfRows: v.rowCount}},
			)
		}
		model := &rowCountCostModel{nodeSegments: nodeSegments}
		suite.InDelta(expects[i], model.cost(), 0.01, "case %d", i+1)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestSegmentCountCostModel() {
	cases := [][]struct {
		nodeID       int64
		segmentCount int
	}{
		{},
		{{0, 10}, {1, 0}},
		{{0, 10}, {1, 10}},
		{{0, 30}, {1, 20}, {2, 10}},
		{{0, 10}},
	}

	expects := []float64{
		0,
		1,
		0,
		0.25,
		0,
	}
	for i, c := range cases {
		nodeSegments := make(map[int64][]*meta.Segment)
		for _, v := range c {
			nodeSegments[v.nodeID] = make([]*meta.Segment, 0)
			for j := 0; j < v.segmentCount; j++ {
				nodeSegments[v.nodeID] = append(nodeSegments[v.nodeID],
					&meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: int64(j)}},
				)
			}
		}
		model := &segmentCountCostModel{nodeSegments: nodeSegments}
		suite.InDelta(expects[i], model.cost(), 0.01, "case %d", i+1)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestBaseGeneratorApplyPlans() {
	distribution := []struct {
		nodeID   []int64
		segments [][]int64
	}{
		{[]int64{0, 1}, [][]int64{{1}, {2}}},
	}

	casePlans := []struct {
		segments []int64
		from     []int64
		to       []int64
	}{
		{[]int64{1}, []int64{0}, []int64{1}},
	}

	expects := []struct {
		nodeID   []int64
		segments [][]int64
	}{
		{[]int64{0, 1}, [][]int64{{}, {1, 2}}},
	}

	for i := 0; i < len(casePlans); i++ {
		nodeSegments := make(map[int64][]*meta.Segment)
		appliedPlans := make([]SegmentAssignPlan, 0)
		d := distribution[i]
		for i, nodeID := range d.nodeID {
			nodeSegments[nodeID] = make([]*meta.Segment, 0)
			for _, segmentID := range d.segments[i] {
				nodeSegments[nodeID] = append(nodeSegments[nodeID],
					&meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: segmentID}},
				)
			}
		}

		p := casePlans[i]
		for j := 0; j < len(p.segments); j++ {
			appliedPlans = append(appliedPlans, SegmentAssignPlan{
				Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: p.segments[i]}},
				From:    p.from[i],
				To:      p.to[i],
			})
		}

		generator := &basePlanGenerator{}
		newNodeSegments := generator.applyPlans(nodeSegments, appliedPlans)
		expected := expects[i]
		for i := 0; i < len(expected.nodeID); i++ {
			newSegmentIDs := lo.FlatMap(newNodeSegments[int64(i)], func(segment *meta.Segment, _ int) []int64 {
				return []int64{segment.ID}
			})
			suite.ElementsMatch(expected.segments[i], newSegmentIDs)
		}
	}
}

func (suite *MultiTargetBalancerTestSuite) TestBaseGeneratorMergePlans() {
	cases := [][2]struct {
		segment []int64
		from    []int64
		to      []int64
	}{
		{{[]int64{1}, []int64{1}, []int64{2}}, {[]int64{1}, []int64{2}, []int64{3}}},
		{{[]int64{1}, []int64{1}, []int64{2}}, {[]int64{2}, []int64{2}, []int64{3}}},
	}

	expects := []struct {
		segment []int64
		from    []int64
		to      []int64
	}{
		{[]int64{1}, []int64{1}, []int64{3}},
		{[]int64{1, 2}, []int64{1, 2}, []int64{2, 3}},
	}

	for i := 0; i < len(cases); i++ {
		planGenerator := &basePlanGenerator{}
		curr := make([]SegmentAssignPlan, 0)
		inc := make([]SegmentAssignPlan, 0)
		for j := 0; j < len(cases[i][0].segment); j++ {
			curr = append(curr, SegmentAssignPlan{
				Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: cases[i][0].segment[j]}},
				From:    cases[i][0].from[j],
				To:      cases[i][0].to[j],
			})
		}
		for j := 0; j < len(cases[i][1].segment); j++ {
			inc = append(inc, SegmentAssignPlan{
				Segment: &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: cases[i][1].segment[j]}},
				From:    cases[i][1].from[j],
				To:      cases[i][1].to[j],
			})
		}

		res := planGenerator.mergePlans(curr, inc)

		var segment []int64
		var from []int64
		var to []int64
		for _, p := range res {
			segment = append(segment, p.Segment.ID)
			from = append(from, p.From)
			to = append(to, p.To)
		}
		suite.ElementsMatch(segment, expects[i].segment, "case %d", i+1)
		suite.ElementsMatch(from, expects[i].from, "case %d", i+1)
		suite.ElementsMatch(to, expects[i].to, "case %d", i+1)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestRowCountPlanGenerator() {
	cases := []struct {
		nodeSegments    map[int64][]*meta.Segment
		expectPlanCount int
		expectCost      float64
	}{
		// case 1
		{
			map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 10}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 10}},
				},
				2: {},
			},
			1,
			0,
		},
		// case 2
		{
			map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 10}},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 10}},
				},
			},
			0,
			0,
		},
	}

	for i, c := range cases {
		generator := newRowCountBasedPlanGenerator(10, false)
		generator.setReplicaNodeSegments(c.nodeSegments)
		generator.setGlobalNodeSegments(c.nodeSegments)
		plans := generator.generatePlans()
		suite.Len(plans, c.expectPlanCount, "case %d", i+1)
		suite.InDelta(c.expectCost, generator.currClusterCost, 0.001, "case %d", i+1)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestSegmentCountPlanGenerator() {
	cases := []struct {
		nodeSegments    map[int64][]*meta.Segment
		expectPlanCount int
		expectCost      float64
	}{
		// case 1
		{
			map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 10}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 10}},
				},
				2: {},
			},
			1,
			0,
		},
		// case 2
		{
			map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 10}},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 10}},
				},
			},
			0,
			0,
		},
	}

	for i, c := range cases {
		generator := newSegmentCountBasedPlanGenerator(10, false)
		generator.setReplicaNodeSegments(c.nodeSegments)
		generator.setGlobalNodeSegments(c.nodeSegments)
		plans := generator.generatePlans()
		suite.Len(plans, c.expectPlanCount, "case %d", i+1)
		suite.InDelta(c.expectCost, generator.currClusterCost, 0.001, "case %d", i+1)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestRandomPlanGenerator() {
	cases := []struct {
		nodeSegments map[int64][]*meta.Segment
		expectCost   float64
	}{
		// case 1
		{
			map[int64][]*meta.Segment{
				1: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 20}}, {SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 30}},
				},
				2: {
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 20}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 10}},
				},
			},
			0,
		},
	}

	for _, c := range cases {
		generator := newRandomPlanGenerator(100) // set a large enough random steps
		generator.setReplicaNodeSegments(c.nodeSegments)
		generator.setGlobalNodeSegments(c.nodeSegments)
		generator.generatePlans()
		suite.InDelta(c.expectCost, generator.currClusterCost, 0.001)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestPlanNoConflict() {
	nodeSegments := make(map[int64][]*meta.Segment)
	totalCount := 0
	// 10 nodes, at most 100 segments, at most 1000 rows
	for i := 0; i < 10; i++ {
		segNum := rand.Intn(99) + 1
		for j := 0; j < segNum; j++ {
			rowCount := rand.Intn(1000)
			nodeSegments[int64(i)] = append(nodeSegments[int64(i)], &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID:        int64(i*1000 + j),
					NumOfRows: int64(rowCount),
				},
			})
			totalCount += rowCount
		}
	}

	balancer := &MultiTargetBalancer{}
	plans := balancer.genPlanByDistributions(nodeSegments, nodeSegments)
	segmentSet := typeutil.NewSet[int64]()
	for _, p := range plans {
		suite.False(segmentSet.Contain(p.Segment.ID))
		segmentSet.Insert(p.Segment.ID)
		suite.NotEqual(p.From, p.To)
	}
}

func (suite *MultiTargetBalancerTestSuite) TestWithComplicateDistribution() {
	f, err := os.OpenFile("/home/sunby/Downloads/8a8c-distribution.csv", os.O_RDONLY, 0644)
	suite.NoError(err)
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	suite.NoError(err)

	collectionServerSegment := make(map[int64]map[int64][]*meta.Segment)
	serverSegmentCount := make(map[int64]int)
	serverRowCount := make(map[int64]int64)

	globalServerSegment := make(map[int64][]*meta.Segment)

	for _, r := range records {
		// serverID,collectionID,partitionID,segmentID,channelName,rowNum,state
		serverID, err := strconv.ParseInt(r[0], 10, 64)
		suite.NoError(err)
		collectionID, err := strconv.ParseInt(r[1], 10, 64)
		suite.NoError(err)
		segmentID, err := strconv.ParseInt(r[3], 10, 64)
		suite.NoError(err)
		rowNum, err := strconv.ParseInt(r[5], 10, 64)
		suite.NoError(err)
		state := r[6]

		// ignore growing for now
		if state == "Growing" {
			continue
		}

		if _, ok := collectionServerSegment[collectionID]; !ok {
			collectionServerSegment[collectionID] = make(map[int64][]*meta.Segment)
		}
		collectionServerSegment[collectionID][serverID] = append(collectionServerSegment[collectionID][serverID],
			&meta.Segment{SegmentInfo: &datapb.SegmentInfo{
				ID:        segmentID,
				NumOfRows: rowNum,
			}},
		)

		globalServerSegment[serverID] = append(globalServerSegment[serverID],
			&meta.Segment{SegmentInfo: &datapb.SegmentInfo{
				ID:        segmentID,
				NumOfRows: rowNum,
			}},
		)

		serverSegmentCount[serverID]++
		serverRowCount[serverID] += rowNum
	}

	// print stats before balance

	fmt.Printf("global stats: \n")
	for serverID, segmentCount := range serverSegmentCount {
		fmt.Printf("server %d, segment count: %d\n", serverID, segmentCount)
	}
	for serverID, rowCount := range serverRowCount {
		fmt.Printf("server %d, row count: %d\n", serverID, rowCount)
	}

	fmt.Println("================================")
	fmt.Println("collection level stats: ")
	for collectionID, serverSegments := range collectionServerSegment {
		fmt.Printf("collection %d: \n", collectionID)
		for serverID, segments := range serverSegments {
			fmt.Printf("server %d, segment count: %d\n", serverID, len(segments))
		}
	}

	for collectionID, serverSegments := range collectionServerSegment {
		fmt.Printf("collection %d: \n", collectionID)
		for serverID, segments := range serverSegments {
			sum := 0
			for _, s := range segments {
				sum += int(s.NumOfRows)
			}
			fmt.Printf("server %d, row count: %d\n", serverID, sum)
		}
	}

	// balance
	allPlans := make([]SegmentAssignPlan, 0)
	collectionServerSegmentsAfterBalance := make(map[int64]map[int64][]*meta.Segment)
	for collectionID, serverSegments := range collectionServerSegment {
		global := make(map[int64][]*meta.Segment)
		for serverID := range serverSegments {
			global[serverID] = globalServerSegment[serverID]
		}
		balancer := &MultiTargetBalancer{}
		plans := balancer.genPlanByDistributions(serverSegments, global)
		for _, p := range plans {
			fmt.Printf("segment %d, from %d to %d\n", p.Segment.ID, p.From, p.To)
		}
		gen := &basePlanGenerator{}
		afterBalance := gen.applyPlans(serverSegments, plans)
		collectionServerSegmentsAfterBalance[collectionID] = afterBalance
		allPlans = append(allPlans, plans...)
	}
	gen := &basePlanGenerator{}
	globalAfterBalance := gen.applyPlans(globalServerSegment, allPlans)

	fmt.Println("================================")
	fmt.Printf("generate %d plans\n", len(allPlans))
	fmt.Printf("global stats after balance: \n")

	for serverID, segments := range globalAfterBalance {
		fmt.Printf("server %d, segment count: %d\n", serverID, len(segments))
	}

	for serverID, segments := range globalAfterBalance {
		sum := 0
		for _, s := range segments {
			sum += int(s.NumOfRows)
		}
		fmt.Printf("server %d, row count: %d\n", serverID, sum)
	}

	fmt.Println("================================")
	fmt.Println("collection level stats: ")
	for collectionID, serverSegments := range collectionServerSegmentsAfterBalance {
		fmt.Printf("collection %d: \n", collectionID)
		for serverID, segments := range serverSegments {
			fmt.Printf("server %d, segment count: %d\n", serverID, len(segments))
		}
	}

	for collectionID, serverSegments := range collectionServerSegmentsAfterBalance {
		fmt.Printf("collection %d: \n", collectionID)
		for serverID, segments := range serverSegments {
			sum := 0
			for _, s := range segments {
				sum += int(s.NumOfRows)
			}
			fmt.Printf("server %d, row count: %d\n", serverID, sum)
		}
	}

}

func TestMultiTargetBalancerTestSuite(t *testing.T) {
	s := new(MultiTargetBalancerTestSuite)
	suite.Run(t, s)
}
