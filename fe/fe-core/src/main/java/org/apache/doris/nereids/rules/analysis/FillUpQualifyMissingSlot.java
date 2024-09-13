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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * fill up missing slot for qualify
 */
public class FillUpQualifyMissingSlot extends FillUpMissingSlots {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.PROJECT_WITH_DISTINCT_TO_AGGREGATE.build(
                logicalQualify(logicalProject())
                    .when(qualify -> qualify.child().isDistinct())
                    .then(qualify -> {
                        LogicalProject<Plan> project = qualify.child();
                        Set<Slot> projectOutputSet = project.getOutputSet();
                        List<NamedExpression> newOutputSlots = Lists.newArrayList();
                        Set<Expression> newConjuncts = new HashSet<>();
                        for (Expression conjunct : qualify.getConjuncts()) {
                            conjunct = conjunct.accept(new DefaultExpressionRewriter<List<NamedExpression>>() {
                                @Override
                                public Expression visitWindow(WindowExpression window, List<NamedExpression> context) {
                                    Alias alias = new Alias(window);
                                    context.add(alias);
                                    return alias.toSlot();
                                }
                            }, newOutputSlots);
                            newConjuncts.add(conjunct);
                        }
                        Set<Slot> notExistedInProject = qualify.getExpressions().stream()
                                .map(Expression::getInputSlots)
                                .flatMap(Set::stream)
                                .filter(s -> !projectOutputSet.contains(s))
                                .collect(Collectors.toSet());

                        newOutputSlots.addAll(notExistedInProject);
                        if (newOutputSlots.isEmpty()) {
                            return null;
                        }
                        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                                .addAll(project.getProjects()).addAll(newOutputSlots).build();
                        LogicalQualify<LogicalProject<Plan>> logicalQualify =
                                new LogicalQualify<>(newConjuncts, new LogicalProject<>(projects, project.child()));
                        return project.withProjects(ImmutableList.copyOf(project.getOutput()))
                                .withChildren(logicalQualify);
                    })
            ),
            RuleType.FILL_UP_HAVING_AGGREGATE.build(
                logicalQualify(logicalProject()).whenNot(qualify -> qualify.child().isDistinct()).then(qualify -> {
                    LogicalProject<Plan> project = qualify.child();
                    Set<Slot> projectOutputSet = project.getOutputSet();
                    List<NamedExpression> newOutputSlots = Lists.newArrayList();
                    Set<Expression> newConjuncts = new HashSet<>();
                    for (Expression conjunct : qualify.getConjuncts()) {
                        conjunct = conjunct.accept(new DefaultExpressionRewriter<List<NamedExpression>>() {
                            @Override
                            public Expression visitWindow(WindowExpression window, List<NamedExpression> context) {
                                Alias alias = new Alias(window);
                                context.add(alias);
                                return alias.toSlot();
                            }
                        }, newOutputSlots);
                        newConjuncts.add(conjunct);
                    }
                    Set<Slot> notExistedInProject = qualify.getExpressions().stream()
                            .map(Expression::getInputSlots)
                            .flatMap(Set::stream)
                            .filter(s -> !projectOutputSet.contains(s))
                            .collect(Collectors.toSet());

                    newOutputSlots.addAll(notExistedInProject);
                    if (newOutputSlots.isEmpty()) {
                        return null;
                    }
                    List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                            .addAll(project.getProjects()).addAll(newOutputSlots).build();
                    return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()),
                            new LogicalQualify<>(newConjuncts, project.withProjects(projects)));
                }
                )),
            RuleType.FILL_UP_HAVING_AGGREGATE.build(
                logicalQualify(aggregate()).then(qualify -> {
                    Aggregate<Plan> agg = qualify.child();
                    FillUpMissingSlots.Resolver resolver = new FillUpMissingSlots.Resolver(agg);
                    qualify.getConjuncts().forEach(resolver::resolve);
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                qualify.getConjuncts(), r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(qualify.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        return notChanged ? qualify.withChildren(a) : new LogicalQualify<>(newConjuncts, a);
                    });
                })
            ),
            RuleType.FILL_UP_HAVING_AGGREGATE.build(
                logicalQualify(logicalHaving(aggregate())).then(qualify -> {
                    LogicalHaving<Aggregate<Plan>> having = qualify.child();
                    Aggregate<Plan> agg = qualify.child().child();
                    FillUpMissingSlots.Resolver resolver = new FillUpMissingSlots.Resolver(agg);
                    qualify.getConjuncts().forEach(resolver::resolve);
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                qualify.getConjuncts(), r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(qualify.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        return notChanged ? qualify.withChildren(having.withChildren(a)) :
                            new LogicalQualify<>(newConjuncts, having.withChildren(a));
                    });
                })
            ),
            RuleType.FILL_UP_HAVING_AGGREGATE.build(
                logicalQualify(logicalHaving(logicalProject())).then(qualify -> {
                    LogicalHaving<LogicalProject<Plan>> having = qualify.child();
                    LogicalProject<Plan> project = qualify.child().child();
                    Set<Slot> projectOutputSet = project.getOutputSet();
                    List<NamedExpression> newOutputSlots = Lists.newArrayList();
                    Set<Expression> newConjuncts = new HashSet<>();
                    for (Expression conjunct : qualify.getConjuncts()) {
                        conjunct = conjunct.accept(new DefaultExpressionRewriter<List<NamedExpression>>() {
                            @Override
                            public Expression visitWindow(WindowExpression window, List<NamedExpression> context) {
                                Alias alias = new Alias(window);
                                context.add(alias);
                                return alias.toSlot();
                            }
                        }, newOutputSlots);
                        newConjuncts.add(conjunct);
                    }
                    Set<Slot> notExistedInProject = qualify.getExpressions().stream()
                            .map(Expression::getInputSlots)
                            .flatMap(Set::stream)
                            .filter(s -> !projectOutputSet.contains(s))
                            .collect(Collectors.toSet());

                    newOutputSlots.addAll(notExistedInProject);
                    if (newOutputSlots.isEmpty()) {
                        return null;
                    }
                    List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                            .addAll(project.getProjects()).addAll(newOutputSlots).build();
                    return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()),
                            new LogicalQualify<>(newConjuncts, having.withChildren(project.withProjects(projects))));
                })
            )
        );
    }
}
