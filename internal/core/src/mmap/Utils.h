// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "log/Log.h"
#include "mmap/Types.h"
#include "storage/Util.h"
#include "common/File.h"

namespace milvus {

inline size_t
WriteFieldData(File& file,
               DataType data_type,
               const FieldDataPtr& data,
               std::vector<std::vector<uint64_t>>& element_indices) {
    size_t total_written{0};
    if (datatype_is_variable(data_type)) {
        switch (data_type) {
            case DataType::VARCHAR:
            case DataType::STRING: {
                for (auto i = 0; i < data->get_num_rows(); ++i) {
                    auto str =
                        static_cast<const std::string*>(data->RawValue(i));
                    ssize_t written = file.Write(str->data(), str->size());
                    if (written < str->size()) {
                        LOG_ERROR(
                            "WriteFieldData failed: written {} bytes, "
                            "expected {} bytes, errno {} {}",
                            written,
                            str->size(),
                            strerror(errno),
                            errno);
                        break;
                    }
                    total_written += written;
                }
                break;
            }
            case DataType::JSON: {
                for (ssize_t i = 0; i < data->get_num_rows(); ++i) {
                    auto padded_string =
                        static_cast<const Json*>(data->RawValue(i))->data();
                    ssize_t written =
                        file.Write(padded_string.data(), padded_string.size());
                    if (written < padded_string.size()) {
                        LOG_ERROR(
                            "WriteFieldData failed: written {} bytes, "
                            "expected {} bytes, errno {} {}",
                            written,
                            padded_string.size(),
                            strerror(errno),
                            errno);
                        break;
                    }
                    total_written += written;
                }
                break;
            }
            case DataType::ARRAY: {
                for (size_t i = 0; i < data->get_num_rows(); ++i) {
                    auto array = static_cast<const Array*>(data->RawValue(i));
                    ssize_t written =
                        file.Write(array->data(), array->byte_size());
                    if (written < array->byte_size()) {
                        LOG_ERROR(
                            "WriteFieldData failed: written {} bytes, "
                            "expected {} bytes, errno {} {}",
                            written,
                            array->byte_size(),
                            strerror(errno),
                            errno);
                        break;
                    }
                    element_indices.emplace_back(array->get_offsets());
                    total_written += written;
                }
                break;
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                // TODO(SPARSE): this is for mmap to write data to disk so that
                // the file can be mmaped into memory.
                throw std::runtime_error(
                    "WriteFieldData for VECTOR_SPARSE_FLOAT not implemented");
            }
            default:
                PanicInfo(DataTypeInvalid,
                          "not supported data type {}",
                          datatype_name(data_type));
        }
    } else {
        auto written = file.Write(data->Data(), data->Size());
        if (written < data->Size()) {
            LOG_ERROR(
                "WriteFieldData failed: written {} bytes, "
                "expected {} bytes, errno {} {}",
                written,
                data->Size(),
                strerror(errno),
                errno);
        }
        total_written += written;
    }

    return total_written;
}
}  // namespace milvus
