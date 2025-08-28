#include "index/ZonemapIndex.h"
#include "log/Log.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace milvus {
namespace index {

namespace {
template <typename T>
struct ToParquetDType;

template <>
struct ToParquetDType<int8_t> {
    using type = parquet::Int32Type;
};

template <>
struct ToParquetDType<int16_t> {
    using type = parquet::Int32Type;
};

template <>
struct ToParquetDType<int32_t> {
    using type = parquet::Int32Type;
};

template <>
struct ToParquetDType<int64_t> {
    using type = parquet::Int64Type;
};

template <>
struct ToParquetDType<float> {
    using type = parquet::FloatType;
};

template <>
struct ToParquetDType<double> {
    using type = parquet::DoubleType;
};
}  // namespace

template <typename T>
std::shared_ptr<ZonemapIndex<T>>
ZonemapIndex<T>::MakeZonemapIndexFromParquetMetadata(
    ParquetFileMetadatas metadatas, int column_id, bool sorted) {
    using cid_t = cachinglayer::cid_t;
    using DType = typename ToParquetDType<T>::type;
    // block_id (cid) must be sequential and match row group index for correct lookup.
    // if cid-row_group_idx mapping changes in the future, we need to change here
    cid_t block_id = 0;
    auto index = std::make_shared<ZonemapIndex<T>>(sorted);

    for (auto& metadata : metadatas) {
        auto num_row_groups = metadata->num_row_groups();
        for (auto i = 0; i < num_row_groups; i++, block_id++) {
            auto row_group = metadata->RowGroup(i);
            auto column_chunk = row_group->ColumnChunk(column_id);
            if (!column_chunk->is_stats_set()) {
                LOG_WARN("Statistics is not set for column chunk {}",
                         column_id);
                return nullptr;
            }
            auto statistics = column_chunk->statistics();
            Statistics stats;
            stats.block_id = block_id;
            stats.has_min_max = statistics->HasMinMax();
            stats.has_null_count = statistics->HasNullCount();
            stats.has_distinct_count = statistics->HasDistinctCount();
            auto typed_statistics =
                std::dynamic_pointer_cast<parquet::TypedStatistics<DType>>(
                    statistics);

            if (!typed_statistics) {
                LOG_WARN("Failed to cast TypedStatistics for column chunk {}",
                         column_id);
                return nullptr;
            }
            if (stats.has_min_max) {
                stats.min = static_cast<T>(typed_statistics->min());
                stats.max = static_cast<T>(typed_statistics->max());
            }
            if (stats.has_null_count) {
                stats.null_count = statistics->null_count();
            }
            if (stats.has_distinct_count) {
                stats.distinct_count = statistics->distinct_count();
            }
            stats.num_values = statistics->num_values();
            index->AppendStatistics(stats);
        }
    }
    return index;
}

template <typename T>
bool
ZonemapIndex<T>::Statistics::contains(const T& value) const {
    if (!has_min_max) {
        return true;
    }
    return value >= min && value <= max;
}

template <typename T>
void
ZonemapIndex<T>::AppendStatistics(const Statistics& statistics) {
    statistics_.push_back(statistics);
}

template <typename T>
void
ZonemapIndexIterator<T>::Seek(const T& value) {
    using Statistics = typename ZonemapIndex<T>::Statistics;
    value_ = value;
    if (index_.sorted_) {
        auto it = std::lower_bound(
            index_.statistics_.begin(),
            index_.statistics_.end(),
            value,
            [](const Statistics& s, const T& v) { return s.max < v; });
        stats_idx_ = std::distance(index_.statistics_.begin(), it);
    } else {
        for (; stats_idx_ < index_.statistics_.size(); stats_idx_++) {
            if (index_.statistics_[stats_idx_].contains(value)) {
                break;
            }
        }
    }
}

template <typename T>
void
ZonemapIndexIterator<T>::Next() {
    if (!Valid()) {
        return;
    }
    if (index_.sorted_) {
        stats_idx_++;
        if (stats_idx_ >= index_.statistics_.size()) {
            return;
        }
        if (!value_.has_value() ||
            index_.statistics_[stats_idx_].contains(value_.value())) {
            return;
        }
        // set stats_idx_ to the end to make Valid() return false
        stats_idx_ = index_.statistics_.size();
    } else {
        for (stats_idx_++; stats_idx_ < index_.statistics_.size();
             stats_idx_++) {
            if (!value_.has_value() ||
                index_.statistics_[stats_idx_].contains(value_.value())) {
                return;
            }
        }
    }
}

template <typename T>
std::size_t
ZonemapIndexIterator<T>::BlockID() const {
    return index_.statistics_[stats_idx_].block_id;
}

template <typename T>
bool
ZonemapIndexIterator<T>::Valid() const {
    return stats_idx_ < index_.statistics_.size();
}

template class ZonemapIndexIterator<int8_t>;
template class ZonemapIndexIterator<int16_t>;
template class ZonemapIndexIterator<int32_t>;
template class ZonemapIndexIterator<int64_t>;
template class ZonemapIndexIterator<float>;
template class ZonemapIndexIterator<double>;
template class ZonemapIndex<int8_t>;
template class ZonemapIndex<int16_t>;
template class ZonemapIndex<int32_t>;
template class ZonemapIndex<int64_t>;
template class ZonemapIndex<float>;
template class ZonemapIndex<double>;

}  // namespace index
}  // namespace milvus