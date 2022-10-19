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

import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * derive additional predicates.
 * for example:
 * a = b and a = 1 => b = 1
 */
public class PredicatePropagation {

    private final Map<Expression, Set<Expression>> equivalentMap = Maps.newHashMap();
    private final Map<Expression, Set<Expression>> predicateMap = Maps.newHashMap();

    public Set<Expression> infer(Set<Expression> predicates) {
        return infer(ExpressionUtils.and(Lists.newArrayList(predicates)));
    }

    public Set<Expression> infer(Expression predicate) {
        List<Expression> splits = ExpressionUtils.extractConjunction(predicate);
        splits.forEach(p -> p.accept(new Visitor(), new InferContext()));
        Set<Slot> inputSlots = predicate.getInputSlots();
        Set<Expression> inferred = Sets.newLinkedHashSet();
        inferred.addAll(splits);
        for (Slot inputSlot : inputSlots) {
            for (Expression search : equivalentSearch(inputSlot)) {
                if (search instanceof EqualTo) {
                    if (!inferred.contains(search) && !inferred.contains(((EqualTo) search).commute())) {
                        inferred.add(search);
                    }
                    continue;
                }
                inferred.add(search);
            }
        }
        splits.forEach(inferred::remove);
        return inferred.stream().filter(p -> p.getInputSlots().size() == 1).collect(Collectors.toSet());
    }

    private static class InferContext {
        Expression parent;

        public Expression getParent() {
            return parent;
        }

        public void setParent(Expression parent) {
            this.parent = parent;
        }
    }

    private class Visitor extends ExpressionVisitor<Void, InferContext> {
        @Override
        public Void visit(Expression expr, InferContext context) {
            return null;
        }

        @Override
        public Void visitEqualTo(EqualTo equalTo, InferContext context) {
            Expression left = equalTo.left();
            Expression right = equalTo.right();
            if (left instanceof Slot && right instanceof Slot) {
                Set<Expression> leftSet = equivalentMap.getOrDefault(left, Sets.newHashSet());
                leftSet.add(right);
                equivalentMap.putIfAbsent(left, leftSet);
                Set<Expression> rightSet = equivalentMap.getOrDefault(right, Sets.newHashSet());
                rightSet.add(left);
                equivalentMap.putIfAbsent(right, rightSet);
            }
            visitComparisonPredicate(equalTo, context);
            return null;
        }

        @Override
        public Void visitComparisonPredicate(ComparisonPredicate cp, InferContext context) {
            Optional<Expression> left = extractSlot(cp.left());
            Optional<Expression> right = extractSlot(cp.right());
            Expression target = null;
            if (left.isPresent() && cp.right().isConstant()) {
                target = left.get();
            } else if (right.isPresent() && cp.left().isConstant()) {
                target = right.get();
            }
            if (Objects.nonNull(target)) {
                Set<Expression> targetSet = predicateMap.getOrDefault(target, Sets.newHashSet());
                addPredicate(cp, targetSet, context);
                predicateMap.putIfAbsent(target, targetSet);
            }
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicate inPredicate, InferContext context) {
            Optional<Expression> cmp = extractSlot(inPredicate.getCompareExpr());
            if (cmp.isPresent() && isAllConstant(inPredicate.getOptions())) {
                Set<Expression> predicates = predicateMap.getOrDefault(cmp.get(), Sets.newHashSet());
                addPredicate(inPredicate, predicates, context);
                predicateMap.putIfAbsent(cmp.get(), predicates);
            }
            return null;
        }

        @Override
        public Void visitNot(Not not, InferContext context) {
            context.setParent(not);
            not.child().accept(this, context);
            return null;
        }

        private void addPredicate(Expression expr, Set<Expression> predicates, InferContext context) {
            if (Objects.nonNull(context.getParent())) {
                predicates.add(context.getParent());
                return;
            }
            predicates.add(expr);
        }

        private boolean isAllConstant(List<Expression> expressions) {
            return expressions.stream().allMatch(Expression::isConstant);
        }

        private Optional<Expression> extractSlot(Expression expression) {
            Set<Slot> inputSlots = expression.getInputSlots();
            if (inputSlots.size() == 1) {
                for (Slot slot : inputSlots) {
                    return Optional.of(slot);
                }
            }
            return Optional.empty();
        }

    }

    private Set<Expression> equivalentSearch(Expression slot) {
        Set<Expression> slots = Sets.newLinkedHashSet();
        slots.add(slot);
        Set<Expression> equivalentSlots = Sets.newLinkedHashSet();
        equivalentSlots(slots, Sets.newHashSet(), equivalentSlots);
        equivalentSlots.remove(slot);
        Set<Expression> result = Sets.newLinkedHashSet();
        for (Expression e : equivalentSlots) {
            result.add(new EqualTo(slot, e));
        }
        result.addAll(equivalentReplace(equivalentSlots, slot));
        return result;
    }

    private void equivalentSlots(Set<Expression> slots, Set<Expression> memo, Set<Expression> result) {
        Set<Expression> tmp = Sets.newLinkedHashSet();
        for (Expression slot : slots) {
            if (!memo.contains(slot)) {
                tmp.addAll(equivalentMap.getOrDefault(slot, Sets.newHashSet()));
            }
        }
        if (tmp.isEmpty()) {
            return;
        }
        result.addAll(tmp);
        memo.addAll(slots);
        slots.clear();
        slots.addAll(tmp);
        equivalentSlots(slots, memo, result);
    }

    private Set<Expression> equivalentReplace(Set<Expression> sourceSlots, Expression target) {
        Map<Expression, Expression> eMap = Maps.newHashMap();
        Set<Expression> result = Sets.newLinkedHashSet();
        for (Expression source : sourceSlots) {
            eMap.put(source, target);
        }
        for (Expression source : sourceSlots) {
            Set<Expression> predicates = predicateMap.getOrDefault(source, Sets.newHashSet());
            predicates.stream().map(p -> ExpressionUtils.replace(p, eMap)).forEach(result::add);
        }
        return result;
    }

//    /**
//     * infer additional predicates.
//     */
//    public Set<Expression> infer(List<Expression> predicates) {
//        Set<Expression> inferred = Sets.newHashSet();
//        for (Expression predicate : predicates) {
//            if (canEquivalentDeduce(predicate)) {
//                List<Expression> newInferred = predicates.stream()
//                        .filter(p -> !p.equals(predicate))
//                        .map(p -> doInfer(predicate, p))
//                        .collect(Collectors.toList());
//                inferred.addAll(newInferred);
//            }
//        }
//        predicates.forEach(inferred::remove);
//        return inferred;
//    }
//
//    private Expression doInfer(Expression leftSlotEqualToRightSlot, Expression expression) {
//        return expression.accept(new DefaultExpressionRewriter<Void>() {
//            @Override
//            public Expression visit(Expression expr, Void context) {
//                expr = super.visit(expr, context);
//
//                // flip leftSlot and rightSlot
//                if (expr.equals(leftSlotEqualToRightSlot.child(0))) {
//                    return leftSlotEqualToRightSlot.child(1);
//                } else if (expr.equals(leftSlotEqualToRightSlot.child(1))) {
//                    return leftSlotEqualToRightSlot.child(0);
//                } else {
//                    return expr;
//                }
//            }
//        }, null);
//    }
//
//    private boolean canEquivalentDeduce(Expression predicate) {
//        return predicate instanceof EqualTo && predicate.children().stream().allMatch(e -> e instanceof SlotReference);
//    }

}
