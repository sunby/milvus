#pragma once
#include <vector>
#include "cachinglayer/Utils.h"
#include "parquet/metadata.h"

namespace milvus {
namespace index {

template <typename T>
class ZonemapIndex;

// ZonemapIndexIterator is an iterator that iterate over the ZonemapIndex
template <typename T>
class ZonemapIndexIterator {
 public:
    ZonemapIndexIterator(const ZonemapIndex<T>& index) : index_(index) {
    }

    // Seeks to the first block that may contain the value
    void
    Seek(const T& value);
    // Seeks to the next block that may contain the value.
    // Returns false if there is no such block
    void
    Next();
    // Get the block id
    std::size_t
    BlockID() const;
    // Returns true if the iterator is valid
    bool
    Valid() const;

 private:
    std::optional<T> value_;
    const ZonemapIndex<T>& index_;
    std::size_t stats_idx_ = 0;
};

class ZonemapIndexBase : public std::enable_shared_from_this<ZonemapIndexBase> {
 public:
    virtual ~ZonemapIndexBase() = default;

    template <typename T>
    std::shared_ptr<ZonemapIndex<T>>
    As() {
        return std::dynamic_pointer_cast<ZonemapIndex<T>>(shared_from_this());
    }
};

// ZonemapIndex is an index that store statistics of a block of data
// It is used to speed up the query of a range of data
// sorted is true when data is sorted which can help reduce the number of blocks to be queried
template <typename T>
class ZonemapIndex : public ZonemapIndexBase {
    friend class ZonemapIndexIterator<T>;
    using ParquetFileMetadatas =
        std::vector<std::shared_ptr<parquet::FileMetaData>>;

 public:
    static std::shared_ptr<ZonemapIndex<T>>
    MakeZonemapIndexFromParquetMetadata(ParquetFileMetadatas metadata,
                                        int column_id,
                                        bool sorted);

    std::shared_ptr<ZonemapIndexIterator<T>>
    NewIterator() {
        return std::make_shared<ZonemapIndexIterator<T>>(*this);
    }

    ZonemapIndex(bool sorted) : sorted_(sorted) {
    }

 private:
    struct Statistics {
        cachinglayer::cid_t block_id;
        T min;
        T max;
        int64_t null_count;
        int64_t num_values;
        int64_t distinct_count;
        bool has_min_max;
        bool has_null_count;
        bool has_distinct_count;

        // Returns true if the range contains the value or has_min_max is false.
        bool
        contains(const T& value) const;
    };

    void
    AppendStatistics(const Statistics& statistics);

    bool sorted_;
    std::vector<Statistics> statistics_;
};

}  // namespace index
}  // namespace milvus