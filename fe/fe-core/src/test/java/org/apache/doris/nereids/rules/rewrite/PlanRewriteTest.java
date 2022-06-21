package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.OptimizerContext;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.jobs.RewriteTopDownJobTest.FakeRule;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.operators.plans.logical.LogicalRelation;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.rewrite.logical.PushDownPredicateIntoScan;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.Optional;


public class PlanRewriteTest implements Plans {

    @Test
    public void pushDownPredicateIntoScanTest() throws AnalysisException {
        Table student = new Table(0L, "student", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                        new Column("age", Type.INT, true, AggregateType.NONE, "", "")));

        Table score = new Table(0L, "score", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("sid", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("cid", Type.INT, true, AggregateType.NONE, "", ""),
                        new Column("score", Type.DOUBLE, true, AggregateType.NONE, "", "")));


        Plan rStudent = plan(new LogicalRelation(student, ImmutableList.of("student")));

        Plan rScore = plan(new LogicalRelation(score, ImmutableList.of("score")));

        Expression onCondition = new EqualTo(rStudent.getOutput().get(0), rScore.getOutput().get(0));
        Expression whereCondition = new GreaterThan(rScore.getOutput().get(2), Literal.of(10));

        Plan join = plan(new LogicalJoin(JoinType.INNER_JOIN, Optional.of(onCondition)), rStudent, rScore);
        Plan filter = plan(new LogicalFilter(whereCondition), join);

        Plan root = plan(new LogicalProject(
                Lists.newArrayList(rStudent.getOutput().get(1), rScore.getOutput().get(1), rScore.getOutput().get(2))),
                filter);

        Memo memo = new Memo();
        memo.initialize(root);

        OptimizerContext optimizerContext = new OptimizerContext(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, null, new PhysicalProperties());
        List<Rule<Plan>> fakeRules = Lists.newArrayList(new PushDownPredicateIntoScan().build());
        RewriteTopDownJob rewriteTopDownJob = new RewriteTopDownJob(memo.getRoot(), fakeRules, plannerContext);
        plannerContext.getOptimizerContext().pushJob(rewriteTopDownJob);
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);

        Group rootGroup = memo.getRoot();
    }
}
