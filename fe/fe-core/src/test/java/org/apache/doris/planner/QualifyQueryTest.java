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

package org.apache.doris.planner;

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class QualifyQueryTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE sales (\n"
                + "   year INT,\n"
                + "   country STRING,\n"
                + "   product STRING,\n"
                + "   profit INT\n"
                + ") \n"
                + "DISTRIBUTED BY HASH(`year`)\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                 + ");");
    }

    @Test
    public void testQualifyQuery() throws Exception {
        connectContext.setDatabase("test");

        String sql = "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk > 1";
        String explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (rk[#7] > 1)"));

        sql = "select year, country, profit from (select * from sales) a where year >= 2000 qualify row_number() over (order by year) > 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#7] > 1)"));


        sql = "select country, sum(profit) as total, row_number() over (order by country) as rk from sales where year >= 2000 group by country having sum(profit) > 100 qualify rk = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (rk[#14] = 1)"));

        sql = "select country, sum(profit) as total from sales where year >= 2000 group by country having sum(profit) > 100 qualify row_number() over (order by country) = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#14] = 1)"));


        sql = "select country, sum(profit) as total, row_number() over (order by country) as rk from sales where year >= 2000 group by country qualify rk = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (rk[#14] = 1)"));

        sql = "select country, sum(profit) as total from sales where year >= 2000 group by country qualify row_number() over (order by country) = 1;";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#14] = 1)"));


        sql = "select year, country, product, profit, row_number() over (partition by year, country order by profit desc) as rk from sales where year >= 2000 qualify rk = 1 order by profit";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (rk[#12] = 1)"));

        sql = "select year, country, product, profit from sales where year >= 2000 qualify row_number() over (partition by year, country order by profit desc) = 1 order by profit";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#12] = 1)"));


        sql = "select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (rk[#10] = 1), (profit[#9] > 200)"));

        sql = "select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#10] = 1), (profit[#9] > 200)"));


        sql = "select distinct year, row_number() over (order by year) as rk from sales group by year qualify rk = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (rk[#7] = 1)"));

        sql = "select distinct year from sales group by year qualify row_number() over (order by year) = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#7] = 1)"));


        sql = "select year, country, profit from (select year, country, profit from (select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200) t where rk = 1) a where year >= 2000 qualify row_number() over (order by profit) = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#20] = 1)"));

        sql = "select year, country, profit from (select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1) a qualify row_number() over (order by profit) = 1";
        explainStr = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql);
        Assert.assertTrue(explainStr.contains("predicates: (_QUALIFY_COLUMN[#20] = 1)"));
    }
}
