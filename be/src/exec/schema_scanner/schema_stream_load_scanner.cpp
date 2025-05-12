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

#include "exec/schema_scanner/schema_stream_load_scanner.h"

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
std::vector<SchemaScanner::ColumnDesc> SchemaStreamLoadScanner::_s_tbls_columns = {
        //   name,       type,          size
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"QUERY_ID", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TXN_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"LABEL", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DATABASE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"USER", TYPE_VARCHAR, sizeof(StringRef), false},
        {"CLIENT_IP", TYPE_VARCHAR, sizeof(StringRef), false},
        {"START_TIME", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaStreamLoadScanner::SchemaStreamLoadScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_STREAM_LOAD) {}

SchemaStreamLoadScanner::~SchemaStreamLoadScanner() {}

Status SchemaStreamLoadScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    return Status::OK();
}

Status SchemaStreamLoadScanner::get_next_block_internal(vectorized::Block* block,
                                                                bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_stream_load_task_block == nullptr) {
        _stream_load_task_block = vectorized::Block::create_unique();

        for (int i = 0; i < _s_tbls_columns.size(); ++i) {
            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(_s_tbls_columns[i].type, true);
            _stream_load_task_block->insert(vectorized::ColumnWithTypeAndName(
                    data_type->create_column(), data_type, _s_tbls_columns[i].name));
        }

        _stream_load_task_block->reserve(_block_rows_limit);

        ExecEnv::GetInstance()->new_load_stream_mgr()->get_all_stream_load_block(_stream_load_task_block.get());
        _total_rows = _stream_load_task_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_stream_load_task_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
