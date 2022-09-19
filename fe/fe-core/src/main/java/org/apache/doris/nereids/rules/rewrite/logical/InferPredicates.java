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

import org.apache.doris.nereids.rules.PlanRuleFactory;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * infer additional predicates for filter and join.
 */
public class InferPredicates implements PlanRuleFactory {
    PredicatePropagation propagation = new PredicatePropagation();
    EffectivePredicatesExtractor predicatesExtractor = new EffectivePredicatesExtractor();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                inferWhere(),
                inferOn()
        );
    }

    private Rule inferWhere() {
        return logicalFilter(any()).thenApply(ctx -> {
            LogicalFilter<Plan> root = ctx.root;
            Plan filter = ctx.cascadesContext.getMemo().copyOut(root.getGroupExpression().get(), false);
            Set<Expression> filterPredicates = filter.accept(predicatesExtractor, null);
            Set<Expression> filterChildPredicates = filter.child(0).accept(predicatesExtractor, null);
            filterPredicates.removeAll(filterChildPredicates);
            ExpressionUtils.extractConjunction(root.getPredicates()).forEach(filterPredicates::remove);
            if (!filterPredicates.isEmpty()) {
                filterPredicates.add(root.getPredicates());
                return new LogicalFilter<>(ExpressionUtils.and(Lists.newArrayList(filterPredicates)), root.child());
            }
            return root;
        }).toRule(RuleType.INFER_PREDICATES_FOR_WHERE);
    }

    private Rule inferOn() {
        return logicalJoin(any(), any()).thenApply(ctx -> {
            LogicalJoin<Plan, Plan> root = ctx.root;
            JoinType joinType = root.getJoinType();
            Plan left = root.left();
            Plan right = root.right();
            Plan originalLeft = ctx.cascadesContext.getMemo().copyOut(left.getGroupExpression().get(), false);
            Plan originalRight = ctx.cascadesContext.getMemo().copyOut(right.getGroupExpression().get(), false);
            Optional<Expression> condition = root.getOnClauseCondition();
            Set<Expression> expressions = getAllExpressions(originalLeft, originalRight, condition);
            switch (joinType) {
                case INNER_JOIN:
                case CROSS_JOIN:
                case LEFT_SEMI_JOIN:
                case RIGHT_SEMI_JOIN:
                    return root.withChildren(inferNewFilter(originalLeft, left, expressions),
                            inferNewFilter(originalRight, right, expressions));
                case LEFT_OUTER_JOIN:
                case LEFT_ANTI_JOIN:
                    return root.withChildren(left, inferNewFilter(originalRight, right, expressions));
                case RIGHT_OUTER_JOIN:
                case RIGHT_ANTI_JOIN:
                    return root.withChildren(inferNewFilter(originalLeft, left, expressions), right);
                default:
                    return root;
            }
        }).toRule(RuleType.INFER_PREDICATES_FOR_ON);
    }

    private Set<Expression> getAllExpressions(Plan left, Plan right, Optional<Expression> condition) {
        Set<Expression> baseExpressions = left.accept(predicatesExtractor, null);
        baseExpressions.addAll(right.accept(predicatesExtractor, null));
        condition.ifPresent(on -> baseExpressions.addAll(ExpressionUtils.extractConjunction(on)));
        baseExpressions.addAll(propagation.infer(Lists.newArrayList(baseExpressions)));
        return baseExpressions;
    }

    private Plan inferNewFilter(Plan originalPlan, Plan plan, Set<Expression> expressions) {
        List<Expression> predicates = expressions.stream()
                .filter(c -> !c.getInputSlots().isEmpty() && new HashSet<>(originalPlan.getOutput()).containsAll(
                        c.getInputSlots())).collect(Collectors.toList());
        predicates.removeAll(originalPlan.accept(predicatesExtractor, null));
        if (!predicates.isEmpty()) {
            return new LogicalFilter<>(ExpressionUtils.and(predicates), plan);
        }
        return plan;
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }
}
