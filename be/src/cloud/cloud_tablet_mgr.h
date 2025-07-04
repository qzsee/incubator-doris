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

#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Types_types.h>

#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"

namespace doris {

class CloudTablet;
class CloudStorageEngine;
class LRUCachePolicy;
class CountDownLatch;
struct SyncRowsetStats;

extern uint64_t g_tablet_report_inactive_duration_ms;

class CloudTabletMgr {
public:
    CloudTabletMgr(CloudStorageEngine& engine);
    ~CloudTabletMgr();

    // If the tablet is in cache, return this tablet directly; otherwise will get tablet meta first,
    // sync rowsets after, and download segment data in background if `warmup_data` is true.
    Result<std::shared_ptr<CloudTablet>> get_tablet(int64_t tablet_id, bool warmup_data = false,
                                                    bool sync_delete_bitmap = true,
                                                    SyncRowsetStats* sync_stats = nullptr,
                                                    bool force_use_cache = false);

    void erase_tablet(int64_t tablet_id);

    void vacuum_stale_rowsets(const CountDownLatch& stop_latch);

    // Return weak ptr of all cached tablets.
    // We return weak ptr to avoid extend lifetime of tablets that are no longer cached.
    std::vector<std::weak_ptr<CloudTablet>> get_weak_tablets();

    void sync_tablets(const CountDownLatch& stop_latch);

    /**
     * Gets top N tablets that are considered to be compacted first
     *
     * @param n max number of tablets to get, all of them are comapction enabled
     * @param filter_out a filter takes a tablet and return bool to check
     *                   whether skipping the tablet, true for skip
     * @param tablets output param
     * @param max_score output param, max score of existed tablets
     * @return status of this call
     */
    Status get_topn_tablets_to_compact(int n, CompactionType compaction_type,
                                       const std::function<bool(CloudTablet*)>& filter_out,
                                       std::vector<std::shared_ptr<CloudTablet>>* tablets,
                                       int64_t* max_score);

    /**
     * Gets tablets info and total tablet num that are reported
     *
     * @param tablets_info used by report
     * @param tablet_num tablets in be tabletMgr, total num
     */
    void build_all_report_tablets_info(std::map<TTabletId, TTablet>* tablets_info,
                                       uint64_t* tablet_num);

    void get_tablet_info(int64_t num_tablets, std::vector<TabletInfo>* tablets_info);

    void get_topn_tablet_delete_bitmap_score(uint64_t* max_delete_bitmap_score,
                                             uint64_t* max_base_rowset_delete_bitmap_score);

    std::vector<std::shared_ptr<CloudTablet>> get_all_tablet();

    // **ATTN: JUST FOR UT**
    void put_tablet_for_UT(std::shared_ptr<CloudTablet> tablet);

private:
    CloudStorageEngine& _engine;

    class TabletMap;
    std::unique_ptr<TabletMap> _tablet_map;
    std::unique_ptr<LRUCachePolicy> _cache;
};

} // namespace doris
