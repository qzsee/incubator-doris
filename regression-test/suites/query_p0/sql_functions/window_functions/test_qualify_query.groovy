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
suite("test_qualify_query") {
    sql "create database if not exists qualify_test"
    sql "use qualify_test"
    sql "DROP TABLE IF EXISTS sales"
    sql """
           CREATE TABLE sales (
               year INT,
               country STRING,
               product STRING,
               profit INT
            ) 
            DISTRIBUTED BY HASH(`year`)
            PROPERTIES (
            "replication_num" = "1"
            )
        """
    sql """
        INSERT INTO sales VALUES
        (2000,'Finland','Computer',1500),
        (2000,'Finland','Phone',100),
        (2001,'Finland','Phone',10),
        (2000,'India','Calculator',75),
        (2000,'India','Calculator',75),
        (2000,'India','Computer',1200),
        (2000,'USA','Calculator',75),
        (2000,'USA','Computer',1500),
        (2001,'USA','Calculator',50),
        (2001,'USA','Computer',1500),
        (2001,'USA','Computer',1200),
        (2001,'USA','TV',150),
        (2001,'USA','TV',100);
        """

    qt_select_1 "select year, country, profit, row_number() over (order by year) as rk from (select * from sales) a where year >= 2000 qualify rk > 1"
    qt_select_2 "select year, country, profit from (select * from sales) a where year >= 2000 qualify row_number() over (order by year) > 1"

    qt_select_3 "select country, sum(profit) as total, row_number() over (order by country) as rk from sales where year >= 2000 group by country having sum(profit) > 100 qualify rk > 1"
    qt_select_4 "select country, sum(profit) as total from sales where year >= 2000 group by country having sum(profit) > 100 qualify row_number() over (order by country) > 1"

    qt_select_5 "select country, sum(profit) as total, row_number() over (order by country) as rk from sales where year >= 2000 group by country qualify rk > 1"
    qt_select_6 "select country, sum(profit) as total from sales where year >= 2000 group by country qualify row_number() over (order by country) > 1"

    qt_select_7 "select year, country, product, profit, row_number() over (partition by year, country order by profit desc) as rk from sales where year >= 2000 qualify rk = 1 order by profit limit 2"
    qt_select_8 "select year, country, product, profit from sales where year >= 2000 qualify row_number() over (partition by year, country order by profit desc) = 1 order by profit limit 2"

    qt_select_9 "select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1 order by profit limit 3"
    qt_select_10 "select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1 order by profit limit 3"

    qt_select_11 "select distinct year, row_number() over (order by year) as rk from sales group by year qualify rk = 1"
    qt_select_12 "select distinct year from sales group by year qualify row_number() over (order by year) = 1"

    qt_select_13 "select year, country, profit from (select year, country, profit from (select year, country, profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200) t where rk = 1) a where year >= 2000 qualify row_number() over (order by profit) = 1"
    qt_select_14 "select year, country, profit from (select year, country, profit from (select * from sales) a where year >= 2000 having profit > 200 qualify row_number() over (partition by year, country order by profit desc) = 1) a qualify row_number() over (order by profit) = 1"

    qt_select_15 "select * except(year) replace( profit+1 as profit), row_number() over (order by year) as rk from sales where year >= 2000 qualify rk > 1"

    qt_select_16 "select * except(year) replace( profit+1 as profit) from sales where year >= 2000 qualify row_number() over (order by year) > 1"

    qt_select_17 "select year + 1, if(country = 'USA', 'usa' , country), case when profit < 200 then 200 else profit end as new_profit, row_number() over (partition by year, country order by profit desc) as rk from (select * from sales) a where year >= 2000 having profit > 200 qualify rk = 1 order by new_profit"

    qt_select_18 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by profit desc, country) = 1 order by country"

    qt_select_19 "select *,row_number() over (partition by year order by profit desc, country) as rk from sales where year >= 2000 qualify rk = 1 order by country"

    qt_select_20 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by if(profit > 200, 1, 0) desc, country) = 1 order by country"

    qt_select_21 "select * from sales where year >= 2000 qualify row_number() over (partition by year order by case when profit > 200 then 1 else 0 end desc, country) = 1 order by country"

    qt_select_22 "select distinct x.year, x.country, x.product from sales x left join sales y on x.year = y.year left join sales z on x.year = z.year where x.year >= 2000 qualify row_number() over (partition by x.year order by x.profit desc) = 1 order by year"
}





