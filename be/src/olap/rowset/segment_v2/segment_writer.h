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

#include <butil/macros.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory> // unique_ptr
#include <string>
#include <vector>

#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace vectorized {
class Block;
class IOlapColumnDataAccessor;
class OlapBlockDataConvertor;
} // namespace vectorized

// TODO(lingbin): Should be a conf that can be dynamically adjusted, or a member in the context
const uint32_t MAX_SEGMENT_SIZE = static_cast<uint32_t>(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE *
                                                        OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE);
class DataDir;
class MemTracker;
class ShortKeyIndexBuilder;
class PrimaryKeyIndexBuilder;
class KeyCoder;
struct RowsetWriterContext;

namespace io {
class FileWriter;
} // namespace io

namespace segment_v2 {

extern const char* k_segment_magic;
extern const uint32_t k_segment_magic_length;

struct SegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
    uint32_t max_rows_per_segment = UINT32_MAX;
    bool enable_unique_key_merge_on_write = false;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;

    RowsetWriterContext* rowset_ctx = nullptr;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
    std::shared_ptr<MowContext> mow_ctx;
};

using TabletSharedPtr = std::shared_ptr<Tablet>;

class SegmentWriter {
public:
    explicit SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                           TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet, DataDir* data_dir,
                           const SegmentWriterOptions& opts, IndexFileWriter* inverted_file_writer);
    ~SegmentWriter();

    Status init();

    // for vertical compaction
    Status init(const std::vector<uint32_t>& col_ids, bool has_key);

    template <typename RowType>
    Status append_row(const RowType& row);

    Status append_block(const vectorized::Block* block, size_t row_pos, size_t num_rows);
    Status probe_key_for_mow(std::string key, std::size_t segment_pos, bool have_input_seq_column,
                             bool have_delete_sign,
                             const std::vector<RowsetSharedPtr>& specified_rowsets,
                             std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                             bool& has_default_or_nullable,
                             std::vector<bool>& use_default_or_null_flag,
                             const std::function<void(const RowLocation& loc)>& found_cb,
                             const std::function<Status()>& not_found_cb,
                             PartialUpdateStats& stats);
    Status partial_update_preconditions_check(size_t row_pos);
    Status append_block_with_partial_content(const vectorized::Block* block, size_t row_pos,
                                             size_t num_rows);
    Status append_block_with_variant_subcolumns(vectorized::Block& data);

    int64_t max_row_to_add(size_t row_avg_size_in_bytes);

    uint64_t estimate_segment_size();

    uint32_t num_rows_written() const { return _num_rows_written; }

    // for partial update
    int64_t num_rows_updated() const { return _num_rows_updated; }
    int64_t num_rows_deleted() const { return _num_rows_deleted; }
    int64_t num_rows_new_added() const { return _num_rows_new_added; }
    int64_t num_rows_filtered() const { return _num_rows_filtered; }

    uint32_t row_count() const { return _row_count; }

    Status finalize(uint64_t* segment_file_size, uint64_t* index_size);

    uint32_t get_segment_id() const { return _segment_id; }

    Status finalize_columns_data();
    Status finalize_columns_index(uint64_t* index_size);
    Status finalize_footer(uint64_t* segment_file_size);

    void init_column_meta(ColumnMetaPB* meta, uint32_t column_id, const TabletColumn& column,
                          TabletSchemaSPtr tablet_schema);
    Slice min_encoded_key();
    Slice max_encoded_key();

    bool is_unique_key() { return _tablet_schema->keys_type() == UNIQUE_KEYS; }

    void clear();

    TabletSchemaSPtr flush_schema() const { return _flush_schema; };

    void set_mow_context(std::shared_ptr<MowContext> mow_context);

    Status close_inverted_index(int64_t* inverted_index_file_size) {
        // no inverted index
        if (_index_file_writer == nullptr) {
            *inverted_index_file_size = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(_index_file_writer->close());
        *inverted_index_file_size = _index_file_writer->get_index_file_total_size();
        return Status::OK();
    }

    uint64_t primary_keys_size() const { return _primary_keys_size; }

private:
    DISALLOW_COPY_AND_ASSIGN(SegmentWriter);
    Status _create_column_writer(uint32_t cid, const TabletColumn& column,
                                 const TabletSchemaSPtr& schema);
    Status _create_writers(const TabletSchemaSPtr& tablet_schema,
                           const std::vector<uint32_t>& col_ids);
    Status _write_data();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_bitmap_index();
    Status _write_inverted_index();
    Status _write_bloom_filter_index();
    Status _write_short_key_index();
    Status _write_primary_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);
    void _maybe_invalid_row_cache(const std::string& key);
    std::string _encode_keys(const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
                             size_t pos);
    // used for unique-key with merge on write and segment min_max key
    std::string _full_encode_keys(
            const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos,
            bool null_first = true);

    std::string _full_encode_keys(
            const std::vector<const KeyCoder*>& key_coders,
            const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos,
            bool null_first = true);

    // used for unique-key with merge on write
    void _encode_seq_column(const vectorized::IOlapColumnDataAccessor* seq_column, size_t pos,
                            std::string* encoded_keys);
    void _encode_rowid(const uint32_t rowid, std::string* encoded_keys);
    void set_min_max_key(const Slice& key);
    void set_min_key(const Slice& key);
    void set_max_key(const Slice& key);
    void _serialize_block_to_row_column(vectorized::Block& block);
    Status _generate_primary_key_index(
            const std::vector<const KeyCoder*>& primary_key_coders,
            const std::vector<vectorized::IOlapColumnDataAccessor*>& primary_key_columns,
            vectorized::IOlapColumnDataAccessor* seq_column, size_t num_rows, bool need_sort);
    Status _generate_short_key_index(std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
                                     size_t num_rows, const std::vector<size_t>& short_key_pos);
    bool _is_mow();
    bool _is_mow_with_cluster_key();

private:
    uint32_t _segment_id;
    TabletSchemaSPtr _tablet_schema;
    BaseTabletSPtr _tablet;
    DataDir* _data_dir = nullptr;
    SegmentWriterOptions _opts;

    // Not owned. owned by RowsetWriter or SegmentFlusher
    io::FileWriter* _file_writer = nullptr;
    // Not owned. owned by RowsetWriter or SegmentFlusher
    IndexFileWriter* _index_file_writer = nullptr;

    SegmentFooterPB _footer;
    // for mow tables with cluster key, the sort key is the cluster keys not unique keys
    // for other tables, the sort key is the keys
    size_t _num_sort_key_columns;
    size_t _num_short_key_columns;

    std::unique_ptr<ShortKeyIndexBuilder> _short_key_index_builder;
    std::unique_ptr<PrimaryKeyIndexBuilder> _primary_key_index_builder;
    std::vector<std::unique_ptr<ColumnWriter>> _column_writers;
    std::unique_ptr<MemTracker> _mem_tracker;

    std::unique_ptr<vectorized::OlapBlockDataConvertor> _olap_data_convertor;
    // used for building short key index or primary key index during vectorized write.
    // for mow table with cluster keys, this is cluster keys
    std::vector<const KeyCoder*> _key_coders;
    // for mow table with cluster keys, this is primary keys
    std::vector<const KeyCoder*> _primary_key_coders;
    const KeyCoder* _seq_coder = nullptr;
    const KeyCoder* _rowid_coder = nullptr;
    std::vector<uint16_t> _key_index_size;
    size_t _short_key_row_pos = 0;

    std::vector<uint32_t> _column_ids;
    bool _has_key = true;
    // _num_rows_written means row count already written in this current column group
    uint32_t _num_rows_written = 0;

    /** for partial update stats **/
    int64_t _num_rows_updated = 0;
    int64_t _num_rows_new_added = 0;
    int64_t _num_rows_deleted = 0;
    // number of rows filtered in strict mode partial update
    int64_t _num_rows_filtered = 0;
    // _row_count means total row count of this segment
    // In vertical compaction row count is recorded when key columns group finish
    //  and _num_rows_written will be updated in value column group
    uint32_t _row_count = 0;

    bool _is_first_row = true;
    faststring _min_key;
    faststring _max_key;

    std::shared_ptr<MowContext> _mow_context;
    // group every rowset-segment row id to speed up reader
    std::map<RowsetId, RowsetSharedPtr> _rsid_to_rowset;
    // contains auto generated columns, should be nullptr if no variants's subcolumns
    TabletSchemaSPtr _flush_schema = nullptr;
    std::vector<std::string> _primary_keys;
    uint64_t _primary_keys_size = 0;
};

} // namespace segment_v2
} // namespace doris
