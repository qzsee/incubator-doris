/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("query54") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    if (isCloudMode()) {
        return
    }
    sql """
         use ${db};
         set enable_nereids_planner=true;
         set enable_nereids_distribute_planner=false;
         set enable_fallback_to_original_planner=false;
         set exec_mem_limit=21G;
         set be_number_for_test=3;
         set parallel_pipeline_task_num=8;
         set forbid_unknown_col_stats=true;
         set enable_nereids_timeout = false;
         set enable_runtime_filter_prune=false;
         set runtime_filter_type=8;
         set dump_nereids_memo=false;
         set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
         set enable_fold_constant_by_be = false;
         set push_topn_to_agg = true;
         set TOPN_OPT_LIMIT_THRESHOLD = 1024;
         set enable_parallel_result_sink=true;
         """
    qt_ds_shape_54 '''
    explain shape plan
    with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from   
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   web_sales
         ) cs_or_ws_sales,
         item,
         date_dim,
         customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Books'
         and i_class = 'business'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 2
         and d_year = 2000
 )
 , my_revenue as (
 select c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   my_customers,
        store_sales,
        customer_address,
        store,
        date_dim
 where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 2000 and d_moy = 2)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 2000 and d_moy = 2)
 group by c_customer_sk
 )
 , segments as
 (select cast((revenue/50) as int) as segment
  from   my_revenue
 )
  select  segment, count(*) as num_customers, segment*50 as segment_base
 from segments
 group by segment
 order by segment, num_customers
 limit 100
    '''
}
