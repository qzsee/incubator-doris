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

import org.junit.Assert;

suite("test_create_mv_mtmv","mtmv") {
    String suiteName = "test_create_mv_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql """
        CREATE MATERIALIZED VIEW mv_mtmv1  as select k2 from ${mvName};
        """

    def max_try_secs = 60
    while (max_try_secs--) {
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${mvName}' ORDER BY CreateTime DESC LIMIT 1; """
        String res = jobStateResult[0][8]
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mv "SELECT * FROM ${mvName}"
    order_qt_sync_mv "SELECT k2 FROM ${mvName}"

    mv_rewrite_success_without_check_chosen("""SELECT k2 FROM ${mvName}""", "mv_mtmv1")

    sql """DROP MATERIALIZED VIEW  mv_mtmv1 ON ${mvName};"""

    order_qt_async_mv "SELECT k2 FROM ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
