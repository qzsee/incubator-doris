// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/file_factory.h"
#include "io/fs/broker_file_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_reader.h"
#include "olap/olap_define.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "vec/common/typeid_cast.h"

namespace doris {
namespace io {

class FileSystem;
struct IOContext;

struct PrefetchRange {
    size_t start_offset;
    size_t end_offset;

    PrefetchRange(size_t start_offset, size_t end_offset)
            : start_offset(start_offset), end_offset(end_offset) {}

    PrefetchRange() : start_offset(0), end_offset(0) {}

    bool operator==(const PrefetchRange& other) const {
        return (start_offset == other.start_offset) && (end_offset == other.end_offset);
    }

    bool operator!=(const PrefetchRange& other) const { return !(*this == other); }

    PrefetchRange span(const PrefetchRange& other) const {
        return {std::min(start_offset, other.end_offset), std::max(start_offset, other.end_offset)};
    }
    PrefetchRange seq_span(const PrefetchRange& other) const {
        return {start_offset, other.end_offset};
    }

    //Ranges needs to be sorted.
    static std::vector<PrefetchRange> merge_adjacent_seq_ranges(
            const std::vector<PrefetchRange>& seq_ranges, int64_t max_merge_distance_bytes,
            int64_t once_max_read_bytes) {
        if (seq_ranges.empty()) {
            return {};
        }
        // Merge overlapping ranges
        std::vector<PrefetchRange> result;
        PrefetchRange last = seq_ranges.front();
        for (size_t i = 1; i < seq_ranges.size(); ++i) {
            PrefetchRange current = seq_ranges[i];
            PrefetchRange merged = last.seq_span(current);
            if (merged.end_offset <= once_max_read_bytes + merged.start_offset &&
                last.end_offset + max_merge_distance_bytes >= current.start_offset) {
                last = merged;
            } else {
                result.push_back(last);
                last = current;
            }
        }
        result.push_back(last);
        return result;
    }
};

class RangeFinder {
public:
    virtual ~RangeFinder() = default;
    virtual Status get_range_for(int64_t desired_offset, io::PrefetchRange& result_range) = 0;
    virtual size_t get_max_range_size() const = 0;
};

class LinearProbeRangeFinder : public RangeFinder {
public:
    LinearProbeRangeFinder(std::vector<io::PrefetchRange>&& ranges) : _ranges(std::move(ranges)) {}

    Status get_range_for(int64_t desired_offset, io::PrefetchRange& result_range) override;

    size_t get_max_range_size() const override {
        size_t max_range_size = 0;
        for (const auto& range : _ranges) {
            max_range_size = std::max(max_range_size, range.end_offset - range.start_offset);
        }
        return max_range_size;
    }

    ~LinearProbeRangeFinder() override = default;

private:
    std::vector<io::PrefetchRange> _ranges;
    size_t index {0};
};

/**
 * The reader provides a solution to read one range at a time. You can customize RangeFinder to meet your scenario.
 * For me, since there will be tiny stripes when reading orc files, in order to reduce the requests to hdfs,
 * I first merge the access to the orc files to be read (of course there is a problem of read amplification,
 * but in my scenario, compared with reading hdfs multiple times, it is faster to read more data on hdfs at one time),
 * and then because the actual reading of orc files is in order from front to back, I provide LinearProbeRangeFinder.
 */
class RangeCacheFileReader : public io::FileReader {
    struct RangeCacheReaderStatistics {
        int64_t request_io = 0;
        int64_t request_bytes = 0;
        int64_t request_time = 0;
        int64_t read_to_cache_time = 0;
        int64_t cache_refresh_count = 0;
        int64_t read_to_cache_bytes = 0;
    };

public:
    RangeCacheFileReader(RuntimeProfile* profile, io::FileReaderSPtr inner_reader,
                         std::shared_ptr<RangeFinder> range_finder);

    ~RangeCacheFileReader() override = default;

    Status close() override {
        if (!_closed) {
            _closed = true;
        }
        return Status::OK();
    }

    const io::Path& path() const override { return _inner_reader->path(); }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    void _collect_profile_before_close() override;

private:
    RuntimeProfile* _profile = nullptr;
    io::FileReaderSPtr _inner_reader;
    std::shared_ptr<RangeFinder> _range_finder;

    OwnedSlice _cache;
    int64_t _current_start_offset = -1;

    size_t _size;
    bool _closed = false;

    RuntimeProfile::Counter* _request_io = nullptr;
    RuntimeProfile::Counter* _request_bytes = nullptr;
    RuntimeProfile::Counter* _request_time = nullptr;
    RuntimeProfile::Counter* _read_to_cache_time = nullptr;
    RuntimeProfile::Counter* _cache_refresh_count = nullptr;
    RuntimeProfile::Counter* _read_to_cache_bytes = nullptr;
    RangeCacheReaderStatistics _cache_statistics;
    /**
     * `RangeCacheFileReader`:
     *   1. `CacheRefreshCount`: how many IOs are merged
     *   2. `ReadToCacheBytes`: how much data is actually read after merging
     *   3. `ReadToCacheTime`: how long it takes to read data after merging
     *   4. `RequestBytes`: how many bytes does the apache-orc library actually need to read the orc file
     *   5. `RequestIO`: how many times the apache-orc library calls this read interface
     *   6. `RequestTime`: how long it takes the apache-orc library to call this read interface
     *
     *   It should be noted that `RangeCacheFileReader` is a wrapper of the reader that actually reads data,such as
     *   the hdfs reader, so strictly speaking, `CacheRefreshCount` is not equal to how many IOs are initiated to hdfs,
     *  because each time the hdfs reader is requested, the hdfs reader may not be able to read all the data at once.
     */
};

/**
 * A FileReader that efficiently supports random access format like parquet and orc.
 * In order to merge small IO in parquet and orc, the random access ranges should be generated
 * when creating the reader. The random access ranges is a list of ranges that order by offset.
 * The range in random access ranges should be reading sequentially, can be skipped, but can't be
 * read repeatedly. When calling read_at, if the start offset located in random access ranges, the
 * slice size should not span two ranges.
 *
 * For example, in parquet, the random access ranges is the column offsets in a row group.
 *
 * When reading at offset, if [offset, offset + 8MB) contains many random access ranges, the reader
 * will read data in [offset, offset + 8MB) as a whole, and copy the data in random access ranges
 * into small buffers(name as box, default 1MB, 128MB in total). A box can be occupied by many ranges,
 * and use a reference counter to record how many ranges are cached in the box. If reference counter
 * equals zero, the box can be release or reused by other ranges. When there is no empty box for a new
 * read operation, the read operation will do directly.
 */
class MergeRangeFileReader : public io::FileReader {
public:
    struct Statistics {
        int64_t copy_time = 0;
        int64_t read_time = 0;
        int64_t request_io = 0;
        int64_t merged_io = 0;
        int64_t request_bytes = 0;
        int64_t merged_bytes = 0;
        int64_t apply_bytes = 0;
    };

    struct RangeCachedData {
        size_t start_offset;
        size_t end_offset;
        std::vector<int16_t> ref_box;
        std::vector<uint32_t> box_start_offset;
        std::vector<uint32_t> box_end_offset;
        bool has_read = false;

        RangeCachedData(size_t start_offset, size_t end_offset)
                : start_offset(start_offset), end_offset(end_offset) {}

        RangeCachedData() : start_offset(0), end_offset(0) {}

        bool empty() const { return start_offset == end_offset; }

        bool contains(size_t offset) const { return start_offset <= offset && offset < end_offset; }

        void reset() {
            start_offset = 0;
            end_offset = 0;
            ref_box.clear();
            box_start_offset.clear();
            box_end_offset.clear();
        }

        int16_t release_last_box() {
            // we can only release the last referenced box to ensure sequential read in range
            if (!empty()) {
                int16_t last_box_ref = ref_box.back();
                uint32_t released_size = box_end_offset.back() - box_start_offset.back();
                ref_box.pop_back();
                box_start_offset.pop_back();
                box_end_offset.pop_back();
                end_offset -= released_size;
                if (empty()) {
                    reset();
                }
                return last_box_ref;
            }
            return -1;
        }
    };

    static constexpr size_t TOTAL_BUFFER_SIZE = 128 * 1024 * 1024;  // 128MB
    static constexpr size_t READ_SLICE_SIZE = 8 * 1024 * 1024;      // 8MB
    static constexpr size_t BOX_SIZE = 1 * 1024 * 1024;             // 1MB
    static constexpr size_t SMALL_IO = 2 * 1024 * 1024;             // 2MB
    static constexpr size_t NUM_BOX = TOTAL_BUFFER_SIZE / BOX_SIZE; // 128

    MergeRangeFileReader(RuntimeProfile* profile, io::FileReaderSPtr reader,
                         const std::vector<PrefetchRange>& random_access_ranges)
            : _profile(profile),
              _reader(std::move(reader)),
              _random_access_ranges(random_access_ranges) {
        _range_cached_data.resize(random_access_ranges.size());
        _size = _reader->size();
        _remaining = TOTAL_BUFFER_SIZE;
        _is_oss = typeid_cast<io::S3FileReader*>(_reader.get()) != nullptr;
        _max_amplified_ratio = config::max_amplified_read_ratio;
        // Equivalent min size of each IO that can reach the maximum storage speed limit:
        // 1MB for oss, 8KB for hdfs
        _equivalent_io_size =
                _is_oss ? config::merged_oss_min_io_size : config::merged_hdfs_min_io_size;
        for (const PrefetchRange& range : _random_access_ranges) {
            _statistics.apply_bytes += range.end_offset - range.start_offset;
        }
        if (_profile != nullptr) {
            const char* random_profile = "MergedSmallIO";
            ADD_TIMER_WITH_LEVEL(_profile, random_profile, 1);
            _copy_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "CopyTime", random_profile, 1);
            _read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ReadTime", random_profile, 1);
            _request_io = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RequestIO", TUnit::UNIT,
                                                       random_profile, 1);
            _merged_io = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MergedIO", TUnit::UNIT,
                                                      random_profile, 1);
            _request_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RequestBytes", TUnit::BYTES,
                                                          random_profile, 1);
            _merged_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MergedBytes", TUnit::BYTES,
                                                         random_profile, 1);
            _apply_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "ApplyBytes", TUnit::BYTES,
                                                        random_profile, 1);
        }
    }

    ~MergeRangeFileReader() override = default;

    Status close() override {
        if (!_closed) {
            _closed = true;
        }
        return Status::OK();
    }

    const io::Path& path() const override { return _reader->path(); }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    // for test only
    size_t buffer_remaining() const { return _remaining; }

    // for test only
    const std::vector<RangeCachedData>& range_cached_data() const { return _range_cached_data; }

    // for test only
    const std::vector<int16_t>& box_reference() const { return _box_ref; }

    // for test only
    const Statistics& statistics() const { return _statistics; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    void _collect_profile_before_close() override {
        if (_profile != nullptr) {
            COUNTER_UPDATE(_copy_time, _statistics.copy_time);
            COUNTER_UPDATE(_read_time, _statistics.read_time);
            COUNTER_UPDATE(_request_io, _statistics.request_io);
            COUNTER_UPDATE(_merged_io, _statistics.merged_io);
            COUNTER_UPDATE(_request_bytes, _statistics.request_bytes);
            COUNTER_UPDATE(_merged_bytes, _statistics.merged_bytes);
            COUNTER_UPDATE(_apply_bytes, _statistics.apply_bytes);
            if (_reader != nullptr) {
                _reader->collect_profile_before_close();
            }
        }
    }

private:
    RuntimeProfile::Counter* _copy_time = nullptr;
    RuntimeProfile::Counter* _read_time = nullptr;
    RuntimeProfile::Counter* _request_io = nullptr;
    RuntimeProfile::Counter* _merged_io = nullptr;
    RuntimeProfile::Counter* _request_bytes = nullptr;
    RuntimeProfile::Counter* _merged_bytes = nullptr;
    RuntimeProfile::Counter* _apply_bytes = nullptr;

    int _search_read_range(size_t start_offset, size_t end_offset);
    void _clean_cached_data(RangeCachedData& cached_data);
    void _read_in_box(RangeCachedData& cached_data, size_t offset, Slice result,
                      size_t* bytes_read);
    Status _fill_box(int range_index, size_t start_offset, size_t to_read, size_t* bytes_read,
                     const IOContext* io_ctx);
    void _dec_box_ref(int16_t box_index);

    RuntimeProfile* _profile = nullptr;
    io::FileReaderSPtr _reader;
    const std::vector<PrefetchRange> _random_access_ranges;
    std::vector<RangeCachedData> _range_cached_data;
    size_t _size;
    bool _closed = false;
    size_t _remaining;

    std::unique_ptr<OwnedSlice> _read_slice;
    std::vector<OwnedSlice> _boxes;
    int16_t _last_box_ref = -1;
    uint32_t _last_box_usage = 0;
    std::vector<int16_t> _box_ref;
    bool _is_oss;
    double _max_amplified_ratio;
    size_t _equivalent_io_size;

    Statistics _statistics;
};

/**
 * Create a file reader suitable for accessing scenarios:
 * 1. When file size < config::in_memory_file_size, create InMemoryFileReader file reader
 * 2. When reading sequential file(csv/json), create PrefetchBufferedReader
 * 3. When reading random access file(parquet/orc), create normal file reader
 */
class DelegateReader {
public:
    enum AccessMode { SEQUENTIAL, RANDOM };

    static Result<io::FileReaderSPtr> create_file_reader(
            RuntimeProfile* profile, const FileSystemProperties& system_properties,
            const FileDescription& file_description, const io::FileReaderOptions& reader_options,
            AccessMode access_mode = SEQUENTIAL, const IOContext* io_ctx = nullptr,
            const PrefetchRange file_range = PrefetchRange(0, 0));
};

class PrefetchBufferedReader;
struct PrefetchBuffer : std::enable_shared_from_this<PrefetchBuffer>, public ProfileCollector {
    enum class BufferStatus { RESET, PENDING, PREFETCHED, CLOSED };

    PrefetchBuffer(const PrefetchRange file_range, size_t buffer_size, size_t whole_buffer_size,
                   io::FileReader* reader, const IOContext* io_ctx,
                   std::function<void(PrefetchBuffer&)> sync_profile)
            : _file_range(file_range),
              _size(buffer_size),
              _whole_buffer_size(whole_buffer_size),
              _reader(reader),
              _io_ctx(io_ctx),
              _buf(new char[buffer_size]),
              _sync_profile(std::move(sync_profile)) {}

    PrefetchBuffer(PrefetchBuffer&& other)
            : _offset(other._offset),
              _file_range(other._file_range),
              _random_access_ranges(other._random_access_ranges),
              _size(other._size),
              _whole_buffer_size(other._whole_buffer_size),
              _reader(other._reader),
              _io_ctx(other._io_ctx),
              _buf(std::move(other._buf)),
              _sync_profile(std::move(other._sync_profile)) {}

    ~PrefetchBuffer() = default;

    size_t _offset {0};
    // [start_offset, end_offset) is the range that can be prefetched.
    // Notice that the reader can read out of [start_offset, end_offset), because FE does not align the file
    // according to the format when splitting it.
    const PrefetchRange _file_range;
    const std::vector<PrefetchRange>* _random_access_ranges = nullptr;
    size_t _size {0};
    size_t _len {0};
    size_t _whole_buffer_size;
    io::FileReader* _reader = nullptr;
    const IOContext* _io_ctx = nullptr;
    std::unique_ptr<char[]> _buf;
    BufferStatus _buffer_status {BufferStatus::RESET};
    std::mutex _lock;
    std::condition_variable _prefetched;
    Status _prefetch_status {Status::OK()};
    std::atomic_bool _exceed = false;
    std::function<void(PrefetchBuffer&)> _sync_profile;
    struct Statistics {
        int64_t copy_time {0};
        int64_t read_time {0};
        int64_t prefetch_request_io {0};
        int64_t prefetch_request_bytes {0};
        int64_t request_io {0};
        int64_t request_bytes {0};
    };
    Statistics _statis;

    // @brief: reset the start offset of this buffer to offset
    // @param: the new start offset for this buffer
    void reset_offset(size_t offset);
    // @brief: start to fetch the content between [_offset, _offset + _size)
    void prefetch_buffer();
    // @brief: used by BufferedReader to read the prefetched data
    // @param[off] read start address
    // @param[buf] buffer to put the actual content
    // @param[buf_len] maximum len trying to read
    // @param[bytes_read] actual bytes read
    Status read_buffer(size_t off, const char* buf, size_t buf_len, size_t* bytes_read);
    // @brief: shut down the buffer until the prior prefetching task is done
    void close();
    // @brief: to detect whether this buffer contains off
    // @param[off] detect offset
    bool inline contains(size_t off) const { return _offset <= off && off < _offset + _size; }

    void set_random_access_ranges(const std::vector<PrefetchRange>* random_access_ranges) {
        _random_access_ranges = random_access_ranges;
    }

    // binary search the last prefetch buffer that larger or include the offset
    int search_read_range(size_t off) const;

    size_t merge_small_ranges(size_t off, int range_index) const;

    void _collect_profile_at_runtime() override {}

    void _collect_profile_before_close() override;
};

constexpr int64_t s_max_pre_buffer_size = 4 * 1024 * 1024; // 4MB

/**
 * A buffered reader that prefetch data in the daemon thread pool.
 *
 * file_range is the range that the file is read.
 * random_access_ranges are the column ranges in format, like orc and parquet.
 *
 * When random_access_ranges is empty:
 * The data is prefetched sequentially until the underlying buffers(4 * 4M as default) are full.
 * When a buffer is read out, it will fetch data backward in daemon, so the underlying reader should be
 * thread-safe, and the access mode of data needs to be sequential.
 *
 * When random_access_ranges is not empty:
 * The data is prefetched order by the random_access_ranges. If some adjacent ranges is small, the underlying reader
 * will merge them.
 */
class PrefetchBufferedReader final : public io::FileReader {
public:
    PrefetchBufferedReader(RuntimeProfile* profile, io::FileReaderSPtr reader,
                           PrefetchRange file_range, const IOContext* io_ctx = nullptr,
                           int64_t buffer_size = -1L);
    ~PrefetchBufferedReader() override;

    Status close() override;

    const io::Path& path() const override { return _reader->path(); }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    void set_random_access_ranges(const std::vector<PrefetchRange>* random_access_ranges) {
        _random_access_ranges = random_access_ranges;
        for (auto& _pre_buffer : _pre_buffers) {
            _pre_buffer->set_random_access_ranges(random_access_ranges);
        }
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    void _collect_profile_before_close() override;

private:
    Status _close_internal();
    size_t get_buffer_pos(int64_t position) const {
        return (position % _whole_pre_buffer_size) / s_max_pre_buffer_size;
    }
    size_t get_buffer_offset(int64_t position) const {
        return (position / s_max_pre_buffer_size) * s_max_pre_buffer_size;
    }
    void reset_all_buffer(size_t position) {
        for (int64_t i = 0; i < _pre_buffers.size(); i++) {
            int64_t cur_pos = position + i * s_max_pre_buffer_size;
            int cur_buf_pos = get_buffer_pos(cur_pos);
            // reset would do all the prefetch work
            _pre_buffers[cur_buf_pos]->reset_offset(get_buffer_offset(cur_pos));
        }
    }

    io::FileReaderSPtr _reader;
    PrefetchRange _file_range;
    const std::vector<PrefetchRange>* _random_access_ranges = nullptr;
    const IOContext* _io_ctx = nullptr;
    std::vector<std::shared_ptr<PrefetchBuffer>> _pre_buffers;
    int64_t _whole_pre_buffer_size;
    bool _initialized = false;
    bool _closed = false;
    size_t _size;
};

/**
 * A file reader that read the whole file into memory.
 * When a file is small(<8MB), InMemoryFileReader can effectively reduce the number of file accesses
 * and greatly improve the access speed of small files.
 */
class InMemoryFileReader final : public io::FileReader {
public:
    InMemoryFileReader(io::FileReaderSPtr reader);

    ~InMemoryFileReader() override;

    Status close() override;

    const io::Path& path() const override { return _reader->path(); }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    void _collect_profile_before_close() override;

private:
    Status _close_internal();
    io::FileReaderSPtr _reader;
    std::unique_ptr<char[]> _data;
    size_t _size;
    bool _closed = false;
};

/**
 * Load all the needed data in underlying buffer, so the caller does not need to prepare the data container.
 */
class BufferedStreamReader {
public:
    struct Statistics {
        int64_t read_time = 0;
        int64_t read_calls = 0;
        int64_t read_bytes = 0;
    };

    /**
     * Return the address of underlying buffer that locates the start of data between [offset, offset + bytes_to_read)
     * @param buf the buffer address to save the start address of data
     * @param offset start offset ot read in stream
     * @param bytes_to_read bytes to read
     */
    virtual Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read,
                              const IOContext* io_ctx) = 0;
    /**
     * Save the data address to slice.data, and the slice.size is the bytes to read.
     */
    virtual Status read_bytes(Slice& slice, uint64_t offset, const IOContext* io_ctx) = 0;
    Statistics& statistics() { return _statistics; }
    virtual ~BufferedStreamReader() = default;
    // return the file path
    virtual std::string path() = 0;

protected:
    Statistics _statistics;
};

class BufferedFileStreamReader : public BufferedStreamReader, public ProfileCollector {
public:
    BufferedFileStreamReader(io::FileReaderSPtr file, uint64_t offset, uint64_t length,
                             size_t max_buf_size);
    ~BufferedFileStreamReader() override = default;

    Status read_bytes(const uint8_t** buf, uint64_t offset, const size_t bytes_to_read,
                      const IOContext* io_ctx) override;
    Status read_bytes(Slice& slice, uint64_t offset, const IOContext* io_ctx) override;
    std::string path() override { return _file->path(); }

protected:
    void _collect_profile_before_close() override {
        if (_file != nullptr) {
            _file->collect_profile_before_close();
        }
    }

private:
    std::unique_ptr<uint8_t[]> _buf;
    io::FileReaderSPtr _file;
    uint64_t _file_start_offset;
    uint64_t _file_end_offset;

    uint64_t _buf_start_offset = 0;
    uint64_t _buf_end_offset = 0;
    size_t _buf_size = 0;
    size_t _max_buf_size;
};

} // namespace io
} // namespace doris
