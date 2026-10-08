package qnview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// syncWarmedSegment is proof of a completed load, not a cache residency lease.
// The requirement is immutable for the segment's load lifecycle. Transparent
// decorators retain access to it via WrappedTransformSegment.
type syncWarmedSegment struct {
	TransformSegment
	epoch int64
}

func (s *syncWarmedSegment) UnwrapTransformSegment() TransformSegment {
	return s.TransformSegment
}

func (s *syncWarmedSegment) QuerySegment() segments.Segment {
	if readable, ok := s.TransformSegment.(ReadableSealedSegment); ok {
		return readable.QuerySegment()
	}
	return nil
}

func (s *syncWarmedSegment) Collection() *segments.Collection {
	if readable, ok := s.TransformSegment.(ReadableSealedSegment); ok {
		return readable.Collection()
	}
	return nil
}

func segmentWarmupEpoch(segment TransformSegment) int64 {
	for segment != nil {
		if warmed, ok := segment.(*syncWarmedSegment); ok {
			return warmed.epoch
		}
		wrapped, ok := segment.(WrappedTransformSegment)
		if !ok {
			break
		}
		segment = wrapped.UnwrapTransformSegment()
	}
	return 0
}

func satisfiesSyncWarmup(segment TransformSegment, epoch int64) bool {
	return segment != nil && (epoch == 0 || segmentWarmupEpoch(segment) == epoch)
}

func validateSyncWarmupRequirement(meta *viewpb.QueryViewMeta) error {
	if meta.GetSyncWarmup() != (meta.GetSyncWarmupEpoch() > 0) || meta.GetSyncWarmupEpoch() < 0 {
		return merr.WrapErrServiceInternalMsg("invalid query view sync warmup requirement")
	}
	return nil
}

// forceSyncLoadInfo never mutates a watch snapshot owned by another consumer.
func forceSyncLoadInfo(info *querypb.SegmentLoadInfo, force bool) *querypb.SegmentLoadInfo {
	if info == nil || !force || info.GetForceSyncWarmup() {
		return info
	}
	clone := proto.Clone(info).(*querypb.SegmentLoadInfo)
	clone.ForceSyncWarmup = true
	return clone
}
