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
suite("q14_1_spill") {
  sql """
    set enable_force_spill=true;
  """
  sql """
    set spill_min_revocable_mem=1;
  """
  sql """
    use regression_test_tpcds_sf1_unique_ck_p1;
  """
  qt_q14_1 """
WITH
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100
"""
}
