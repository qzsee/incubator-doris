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
suite("merge_percentile_to_array") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
          DROP TABLE IF EXISTS test_merge_percentile
         """

    sql """
         create table test_merge_percentile(pk int, a int, b int) distributed by hash(pk) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """
         insert into test_merge_percentile values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
        ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8);
      """

    order_qt_merge_two "select sum(a),percentile(pk, 0.1) as c1 , percentile(pk, 0.2) as c2 from test_merge_percentile;"
    order_qt_merge_three """select sum(a),percentile(pk, 0.1) as c1 , percentile(pk, 0.2) as c2 ,
            percentile(pk, 0.4) as c2 from test_merge_percentile;"""
    order_qt_merge_two_group """select sum(a),percentile(pk, 0.1) , percentile(pk, 0.2),
            percentile(a, 0.1),percentile(a, 0.55) as c2 from test_merge_percentile;"""
    order_qt_merge_two_group """select sum(a),percentile(pk, 0.1) as c1 , percentile(a, 0.2) c2,
            percentile(pk, 0.1) c3, percentile(a, 0.55) as c4 from test_merge_percentile;"""
    order_qt_no_merge "select sum(a),percentile(pk, 0.1)  from test_merge_percentile;"
    order_qt_with_group_by """select sum(a),percentile(pk, 0.1) as c1 , percentile(pk, 0.2) as c2 
            from test_merge_percentile group by b;"""

    order_qt_with_upper_refer """select c1, c2 from (
            select sum(a),percentile(pk, 0.1) as c1 , percentile(a, 0.2),percentile(pk, 0.1),
            percentile(a, 0.55) as c2 from test_merge_percentile) t;
    """
    order_qt_with_expr """
            select c1, c2 from (
            select sum(a),percentile(pk+1, 0.1) as c1 , percentile(abs(a), 0.2),percentile(pk+1, 0.3),
            percentile(abs(a), 0.55) as c2 from test_merge_percentile) t;
    """

    order_qt_no_other_agg_func """select c1, c2, a from (
            select a, percentile(pk+1, 0.1) as c1 , percentile(abs(a), 0.2),percentile(pk+1, 0.3),
            percentile(abs(a), 0.55) as c2 from test_merge_percentile group by a) t;      
    """

    order_qt_same_percentile """select sum(a),percentile(pk, 0.1) as c1 , percentile(pk, 0.1) as c2 ,
            percentile(pk, 0.4) as c2 from test_merge_percentile;"""
    order_qt_same_percentile_group_by """select sum(a),percentile(pk, 0.1) as c1 , percentile(pk, 0.1) as c2 ,
            percentile(pk, 0.4) as c2 from test_merge_percentile group by a;"""

    order_qt_grouping """
    select a,percentile(pk, 0.1),percentile(pk, 0.9) from test_merge_percentile group by grouping sets((a,b),(a),())
    """
    sql "set debug_skip_fold_constant=true;"
    order_qt_skip_fold """
    select a,b,percentile(pk, 0.1+0.2),percentile(pk, 0.9),percentile(pk, 0.6) from test_merge_percentile group by a,b
    """
    order_qt_grouping_skip_fold "SELECT percentile(a, 0.11), percentile(a,0.25) as percentiles FROM test_merge_percentile ts  group by grouping sets((b),(pk),())"
    order_qt_grouping_expr "SELECT percentile(a, 0.11), percentile(a,0.25+0.1) as percentiles FROM test_merge_percentile ts  group by grouping sets((b),(pk),())"
    order_qt_grouping_expr_other_agg "SELECT percentile(a, 0.11), percentile(a,0.25+0.1) as percentiles, min(a) FROM test_merge_percentile ts  group by grouping sets((b),(a),())"
    order_qt_grouping_expr_other_agg_upper_ref """select c1,c3 from (SELECT percentile(a, 0.11) c1, percentile(a,0.25+0.1) as c2, min(a) c3
    FROM test_merge_percentile ts group by grouping sets((b),(pk),())) t"""
    order_qt_grouping_expr_other_agg_upper_ref_multi_transform """select percentile(c1,0.5),percentile(c1,0.25), c2 from 
    (SELECT percentile(a, 0.11) c1, percentile(a,0.25+0.1) as c2, min(a) c3
    FROM test_merge_percentile ts group by grouping sets((b),(pk),())) t group by c2"""
    order_qt_grouping_multi_merge """
    select percentile(c1,0.5), percentile(c1,0.25), percentile(c2,0.1),percentile(c2,0.1+0.6) from 
    (SELECT percentile(a, 0.11) c1, percentile(a,0.25+0.1) as c2, min(a) c3
    FROM test_merge_percentile ts group by grouping sets((b),(pk),())) t group by c2
    """
}