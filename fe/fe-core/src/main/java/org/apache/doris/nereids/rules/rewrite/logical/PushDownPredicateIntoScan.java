package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalRelation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionUtils;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.util.SlotExtractor;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PushDownPredicateIntoScan extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalFilter(logicalJoin()).then(plan -> {

            Expression filterPredicates = plan.operator.getPredicates();
            Optional<Expression> onPredicates = plan.child().operator.getOnClause();

            List<Slot> leftOutput = plan.child().left().getOutput();
            List<Slot> rightOutput = plan.child().right().getOutput();

            List<Expression> joinConditions = Lists.newArrayList();
            List<Expression> remainingConjuncts = Lists.newArrayList();

            ExpressionUtils.extractConjunct(onPredicates.get()).forEach(predicate -> {
                if (Objects.nonNull(getJoinCondition(predicate, leftOutput, rightOutput))) {
                    joinConditions.add(predicate);
                } else {
                    remainingConjuncts.add(predicate);
                }
            });
            remainingConjuncts.addAll(ExpressionUtils.extractConjunct(filterPredicates));
            Plan pushed = pushDownPredicate(plan.child(), remainingConjuncts, leftOutput, rightOutput);
            if (pushed != plan.child()) {
                return plan.withChildren(Lists.newArrayList(pushed));
            }
            return plan;
        }).toRule(RuleType.PUSH_DOWN_PREDICATE_INTO_SCAN);
    }

    private Plan pushDownPredicate(LogicalBinaryPlan<LogicalJoin, Plan, Plan> plan,
            List<Expression> conjuncts, List<Slot> leftOutput, List<Slot> rightOutput) {

        List<Expression> leftPredicates = Lists.newArrayList();
        List<Expression> rightPredicates = Lists.newArrayList();

        for (Expression conjunct : conjuncts) {
            List<Slot> slots = SlotExtractor.extractSlot(conjunct);

            if (slots.isEmpty()) {
                leftPredicates.add(conjunct);
                rightPredicates.add(conjunct);
                continue;
            }
            if (leftOutput.containsAll(slots)) {
                leftPredicates.add(conjunct);
            }
            if (rightOutput.containsAll(slots)) {
                rightPredicates.add(conjunct);
            }
        }

        Expression left = ExpressionUtils.add(leftPredicates);
        Expression right = ExpressionUtils.add(rightPredicates);

        if (!left.equals(ExpressionUtils.TRUE_LITERAL)) {
            return plan.withChildren(Lists.newArrayList(plan(new LogicalFilter(left), plan.left()), plan.right()));
        }

        if (!right.equals(ExpressionUtils.TRUE_LITERAL)) {
            return plan.withChildren(Lists.newArrayList(plan.left(), plan(new LogicalFilter(right), plan.right())));
        }
        return plan;
    }

    private Expression getJoinCondition(Expression predicate, List<Slot> leftOutput,
            List<Slot> rightOutput) {
        if (!(predicate instanceof ComparisonPredicate)) {
            return null;
        }

        ComparisonPredicate comparison = (ComparisonPredicate) predicate;

        if (!(comparison.left() instanceof Slot) || !(comparison.right() instanceof Slot)) {
            return null;
        }

        Slot left = (Slot) comparison.left();
        Slot right = (Slot) comparison.right();

        if (!leftOutput.contains(left)) {
            Slot tmp = left;
            left = right;
            right = tmp;
        }

        if (leftOutput.contains(left) && rightOutput.contains(right)) {
            return predicate;
        }

        return null;
    }
}
