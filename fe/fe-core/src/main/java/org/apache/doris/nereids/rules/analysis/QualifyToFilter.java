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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HavingToFilter
 */
public class QualifyToFilter extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalQualify()
                .then(qualify -> {

                    Set<String> windowSlotName = new HashSet<>();
                    for (Expression conjunct : qualify.getConjuncts()) {
                        conjunct.accept(new DefaultExpressionVisitor<Void, Set<String>>() {
                            @Override
                            public Void visitSlotReference(SlotReference slotReference, Set<String> context) {
                                context.add(slotReference.getName());
                                return null;
                            }

                        }, windowSlotName);
                    }
                    AtomicBoolean hasWindow = new AtomicBoolean(false);
                    qualify.accept(new DefaultPlanVisitor<Void, Void>() {
                        private void findWindow(NamedExpression namedExpression) {
                            if (namedExpression instanceof Alias) {
                                if (namedExpression.child(0) instanceof WindowExpression) {
                                    if (windowSlotName.contains(namedExpression.getName())) {
                                        hasWindow.set(true);
                                    }
                                }
                            }
                        }

                        @Override
                        public Void visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
                            for (NamedExpression namedExpression : project.getProjects()) {
                                findWindow(namedExpression);
                            }
                            return visit(project, context);
                        }

                        @Override
                        public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
                            for (NamedExpression namedExpression : aggregate.getOutputExpressions()) {
                                findWindow(namedExpression);
                            }
                            return visit(aggregate, context);
                        }
                    }, null);
                    if (!hasWindow.get()) {
                        throw new AnalysisException("qualify only use for window expression");
                    }
                    return new LogicalFilter<>(qualify.getConjuncts(), qualify.child());
                }).toRule(RuleType.QUALIFY_TO_FILTER);
    }
}
