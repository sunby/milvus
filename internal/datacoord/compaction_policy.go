package datacoord

import (
	"sort"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type singleCompactionPolicy interface {
	// shouldSingleCompaction generates a compaction plan for single comapction, return nil if no plan can be generated.
	generatePlan(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan
}

type mergeCompactionPolicy interface {
	// shouldMergeCompaction generates a compaction plan for merge compaction, return nil if no plan can be generated.
	generatePlan(segments []*SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan
}

type singleCompactionFunc func(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan

func (f singleCompactionFunc) generatePlan(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan {
	return f(segment, timetravel)
}

func chooseAllBinlogs(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan {
	deltaLogs := make([]*datapb.DeltaLogInfo, 0)
	for _, l := range segment.GetDeltalogs() {
		if l.TimestampTo < timetravel.time {
			deltaLogs = append(deltaLogs, l)
		}
	}

	return &datapb.CompactionPlan{
		MergeGroup: []*datapb.CompactionMergeGroup{
			{SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           segment.GetID(),
					FieldBinlogs:        segment.GetBinlogs(),
					Field2StatslogPaths: segment.GetStatslogs(),
					Deltalogs:           deltaLogs,
				},
			}},
		},
		Type:       datapb.CompactionType_InnerCompaction,
		Timetravel: timetravel.time,
	}
}

type mergeCompactionFunc func(segments []*SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan

func (f mergeCompactionFunc) generatePlan(segments []*SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan {
	return f(segments, timetravel)
}

func greedyMergeCompaction(segments []*SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan {
	if len(segments) == 0 {
		return nil
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].NumOfRows < segments[j].NumOfRows
	})

	mergeGroups := greedyGenerateMergeGroups(segments)
	if len(mergeGroups) == 0 {
		return nil
	}

	return &datapb.CompactionPlan{
		Timetravel: timetravel.time,
		Type:       datapb.CompactionType_MergeCompaction,
		MergeGroup: mergeGroups,
	}
}

func greedyGenerateMergeGroups(sortedSegments []*SegmentInfo) []*datapb.CompactionMergeGroup {
	maxRowNumPerSegment := sortedSegments[0].MaxRowNum

	mergeGroups := make([]*datapb.CompactionMergeGroup, 0)
	free := maxRowNumPerSegment
	mergeGroup := &datapb.CompactionMergeGroup{}

	for _, s := range sortedSegments {
		segmentBinlogs := &datapb.CompactionSegmentBinlogs{
			SegmentID:    s.GetID(),
			FieldBinlogs: s.GetBinlogs(),
		}

		if s.NumOfRows > free {
			// if the merge group size is less than or equal to 1, it means that every unchecked segment is larger than half of max segment size
			// so there's no need to merge them
			if len(mergeGroup.SegmentBinlogs) <= 1 {
				break
			}
			mergeGroups = append(mergeGroups, mergeGroup)
			mergeGroup = &datapb.CompactionMergeGroup{}
			free = maxRowNumPerSegment
		}
		mergeGroup.SegmentBinlogs = append(mergeGroup.SegmentBinlogs, segmentBinlogs)
		free -= s.GetNumOfRows()
	}

	// if merge group contains zero or one segment, dont need to merge
	if len(mergeGroup.SegmentBinlogs) > 1 {
		mergeGroups = append(mergeGroups, mergeGroup)
	}

	return mergeGroups
}
