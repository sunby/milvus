package segments

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

type cntReducer struct{}

func (r *cntReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	cnt := int64(0)
	allRetrieveCount := int64(0)
	relatedDataSize := int64(0)
	for _, res := range results {
		allRetrieveCount += res.GetAllRetrieveCount()
		relatedDataSize += res.GetCostAggregation().GetTotalRelatedDataSize()
		c, err := funcutil.CntOfInternalResult(res)
		if err != nil {
			return nil, err
		}
		cnt += c
	}
	res := funcutil.WrapCntToInternalResult(cnt)
	res.AllRetrieveCount = allRetrieveCount
	res.CostAggregation = &internalpb.CostAggregation{
		TotalRelatedDataSize: relatedDataSize,
	}
	return res, nil
}

type cntReducerSegCore struct{}

func (r *cntReducerSegCore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, segments []Segment, _ *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	cnt := int64(0)
	allRetrieveCount := int64(0)
	for i, res := range results {
		allRetrieveCount += res.GetAllRetrieveCount()
		c, err := funcutil.CntOfSegCoreResult(res)
		if err != nil {
			return nil, err
		}
		log.Info("[remove me] segment count reduce", zap.Int("segment id", int(segments[i].GetID())), zap.Int64("count", c))
		cnt += c
	}
	res := funcutil.WrapCntToSegCoreResult(cnt)
	res.AllRetrieveCount = allRetrieveCount
	return res, nil
}
