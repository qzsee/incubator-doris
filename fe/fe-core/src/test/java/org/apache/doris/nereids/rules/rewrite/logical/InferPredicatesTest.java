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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class InferPredicatesTest extends TestWithFeService implements PatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n"
                + "id int not null,\n"
                + "name varchar(128),\n"
                + "age int,sex int)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n"
                + "sid int not null, \n"
                + "cid int not null, \n"
                + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.course (\n"
                + "id int not null, \n"
                + "name varchar(128), \n"
                + "teacher varchar(128))\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        connectContext.setDatabase("default_cluster:test");
    }

    @Test
    public void inferPredicatesTest01() {
        String sql = "select * from student join score on student.id = score.sid where student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest02() {
        String sql = "select * from student join score on student.id = score.sid";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalOlapScan()
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest03() {
        String sql = "select * from student join score on student.id = score.sid where student.id in (1,2,3)";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id IN (1, 2, 3)")),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid IN (1, 2, 3)")))));
    }

    @Test
    public void inferPredicatesTest04() {
        String sql = "select * from student join score on student.id = score.sid and student.id in (1,2,3)";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id IN (1, 2, 3)")),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid IN (1, 2, 3)")))));
    }

    @Test
    public void inferPredicatesTest05() {
        String sql = "select * from student join score on student.id = score.sid join course on score.sid = course.id where student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalJoin(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("sid > 1"))
                                        ),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest06() {
        String sql = "select * from student join score on student.id = score.sid join course on score.sid = course.id and score.sid > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalJoin(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("sid > 1"))
                                        ),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest07() {
        String sql = "select * from student left join score on student.id = score.sid where student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest08() {
        String sql = "select * from student left join score on student.id = score.sid and student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest09() {
        // convert left join to inner join
        String sql = "select * from student left join score on student.id = score.sid where score.sid > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("id > 1")),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest10() {
        String sql = "select * from (select id as nid, name from student) t left join score on t.nid = score.sid where t.nid > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filer -> filer.getPredicates().toSql().contains("id > 1"))
                                        ),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest11() {
        String sql = "select * from (select id as nid, name from student) t left join score on t.nid = score.sid and t.nid > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalOlapScan()
                                        ),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest12() {
        String sql = "select * from student left join (select sid as nid, sum(grade) from score group by sid) s on s.nid = student.id where student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("id > 1")),
                                        logicalProject(
                                                logicalFilter(
                                                        logicalAggregate(
                                                                logicalProject(
                                                                        logicalOlapScan()
                                                                )
                                                        )
                                                ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                        )
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest13() {
        String sql = "select * from (select id, name from student where id = 1) t left join score on t.id = score.sid";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filer -> filer.getPredicates().toSql().contains("id = 1"))
                                        ),
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filer -> filer.getPredicates().toSql().contains("sid = 1"))
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest14() {
        String sql = "select * from student left semi join score on student.id = score.sid where student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                        )
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest15() {
        String sql = "select * from student left semi join score on student.id = score.sid and student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filer -> filer.getPredicates().toSql().contains("sid > 1"))
                                        )
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest16() {
        String sql = "select * from student left anti join score on student.id = score.sid and student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("sid > 1"))
                                        )
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest17() {
        String sql = "select * from student left anti join score on student.id = score.sid and score.sid > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalOlapScan(),
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("sid > 1"))
                                        )
                                )
                        )
                );
    }

    @Test
    public void inferPredicatesTest18() {
        String sql = "select * from student left anti join score on student.id = score.sid where student.id > 1";
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        System.out.println(plan.treeString());
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalProject(
                                logicalJoin(
                                        logicalFilter(
                                                logicalOlapScan()
                                        ).when(filter -> filter.getPredicates().toSql().contains("id > 1")),
                                        logicalProject(
                                                logicalFilter(
                                                        logicalOlapScan()
                                                ).when(filter -> filter.getPredicates().toSql().contains("sid > 1"))
                                        )
                                )
                        )
                );
    }
}
