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

import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * infer additional predicates for filter and join.
 */
public class InferPredicates implements RewriteRuleFactory {
    PredicatePropagation propagation = new PredicatePropagation();
    PullUpPredicates predicatesExtractor = new PullUpPredicates();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                inferWhere(),
                inferOn()
        );
    }

    private Rule inferWhere() {
        return logicalFilter(any()).thenApply(ctx -> {
            if (!ctx.connectContext.getSessionVariable().isEnableInferPredicate()) {
                return null;
            }
            LogicalFilter<Plan> root = ctx.root;
            Plan filter = getOriginalPlan(ctx, root);
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
            if (!ctx.connectContext.getSessionVariable().isEnableInferPredicate()) {
                return null;
            }
            LogicalJoin<Plan, Plan> root = ctx.root;
            JoinType joinType = root.getJoinType();
            Plan left = root.left();
            Plan right = root.right();
            Plan originalLeft = getOriginalPlan(ctx, left);
            Plan originalRight = getOriginalPlan(ctx, right);
            Optional<Expression> condition = root.getOnClauseCondition();
            Set<Expression> expressions = getAllExpressions(originalLeft, originalRight, condition);
            List<Expression> otherJoinConjuncts = Lists.newArrayList(root.getOtherJoinConjuncts());
            switch (joinType) {
                case INNER_JOIN:
                case CROSS_JOIN:
                case LEFT_SEMI_JOIN:
                case RIGHT_SEMI_JOIN:
                    otherJoinConjuncts.addAll(inferNewPredicate(originalLeft, expressions));
                    otherJoinConjuncts.addAll(inferNewPredicate(originalRight, expressions));
                    break;
                case LEFT_OUTER_JOIN:
                case LEFT_ANTI_JOIN:
                    otherJoinConjuncts.addAll(inferNewPredicate(originalRight, expressions));
                    break;
                case RIGHT_OUTER_JOIN:
                case RIGHT_ANTI_JOIN:
                    otherJoinConjuncts.addAll(inferNewPredicate(originalLeft, expressions));
                    break;
                default:
                    return root;
            }
            return root.withOtherJoinConjuncts(otherJoinConjuncts);
        }).toRule(RuleType.INFER_PREDICATES_FOR_ON);
    }

    private Plan getOriginalPlan(MatchingContext context, Plan patternPlan) {
        return context.cascadesContext.getMemo().copyOut(patternPlan.getGroupExpression().get(), false);
    }

    private Set<Expression> getAllExpressions(Plan left, Plan right, Optional<Expression> condition) {
        Set<Expression> baseExpressions = left.accept(predicatesExtractor, null);
        baseExpressions.addAll(right.accept(predicatesExtractor, null));
        condition.ifPresent(on -> baseExpressions.addAll(ExpressionUtils.extractConjunction(on)));
        baseExpressions.addAll(propagation.infer(Lists.newArrayList(baseExpressions)));
        return baseExpressions;
    }

    private List<Expression> inferNewPredicate(Plan originalPlan, Set<Expression> expressions) {
        List<Expression> predicates = expressions.stream()
                .filter(c -> !c.getInputSlots().isEmpty() && originalPlan.getOutputSet().containsAll(
                        c.getInputSlots())).collect(Collectors.toList());
        predicates.removeAll(originalPlan.accept(predicatesExtractor, null));
        return predicates;
    }
}

