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

package org.apache.doris.nereids.rules.rewrite.physical;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.PlanPostprocessor;
import org.apache.doris.nereids.rules.rewrite.physical.RuntimeFilter.RuntimeFilterTarget;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.Lists;

import java.util.List;

public class GenerateRuntimeFilter extends PlanPostprocessor {

    private final IdGenerator<RuntimeFilterId> filterIdGenerator = RuntimeFilterId.createGenerator();

    @Override
    public Plan visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, CascadesContext context) {
        Plan plan = hashJoin;
        if (canGenerate(hashJoin)) {

            List<EqualTo> eqConditions = JoinUtils.getEqualTo(hashJoin);

            // todo: src expr type can't be bitmap or hll
            List<PhysicalOlapScan> scans = hashJoin.left().collect(PhysicalOlapScan.class::isInstance);

            List<EqualTo> validEqConditions = Lists.newArrayList();
            for (EqualTo eqCondition : eqConditions) {
                List<SlotReference> targetSlots = eqCondition.left().collect(SlotReference.class::isInstance);
                for (PhysicalOlapScan scan : scans) {
                    List<Slot> scanOutput = scan.getOutput();
                    if (scanOutput.containsAll(targetSlots)) {
                        validEqConditions.add(eqCondition);
                    }
                }
            }

            List<RuntimeFilter> filters = Lists.newArrayList();
            for (TRuntimeFilterType type : TRuntimeFilterType.values()) {
                if ((context.getConnectContext().getSessionVariable().getRuntimeFilterType() & type.getValue()) == 0) {
                    continue;
                }
                for (int i = 0; i < validEqConditions.size(); i++) {
                    EqualTo conjunct = validEqConditions.get(i);
                    RuntimeFilter filter = new RuntimeFilter(filterIdGenerator.getNextId(), conjunct.right(), conjunct.left(), type);
                    filters.add(filter);
                }
            }

            if (!filters.isEmpty()) {
                Plan left = assignFilter(filters, hashJoin.left());
                plan = hashJoin.addRuntimeFilter(filters).withChildren(Lists.newArrayList(left, hashJoin.right()));
            }
        }
        Plan left = plan.child(0).accept(this, context);
        Plan right = plan.child(1).accept(this, context);

        return plan.withChildren(Lists.newArrayList(left, right));
    }

    private boolean canGenerate(AbstractPhysicalJoin<Plan, Plan> hashJoin) {
        return !hashJoin.getJoinType().isLeftOuterJoin() && !hashJoin.getJoinType().isFullOuterJoin()
                && !hashJoin.getJoinType().isAntiJoin();
    }

    private Plan assignFilter(List<RuntimeFilter> filters, Plan root) {
        return root.accept(new Visitor(filters), new RuntimeFilterContext(null));
    }

    public static class Visitor extends DefaultPlanRewriter<RuntimeFilterContext> {

        private List<RuntimeFilter> filters;

        public Visitor(List<RuntimeFilter> filters) {
            this.filters = filters;
        }

        @Override
        public Plan visitPhysicalOlapScan(PhysicalOlapScan olapScan, RuntimeFilterContext context) {
            List<RuntimeFilter> assign = Lists.newArrayList();
            for (RuntimeFilter filter : filters) {
                List<SlotReference> targetSlots = filter.getTargetExpr().collect(SlotReference.class::isInstance);
                if (olapScan.getOutput().containsAll(targetSlots)) {

                    Expression targetExpr = filter.getTargetExpr();
                    Expression srcExpr = filter.getSrcExpr();
                    if (!targetExpr.getDataType().equals(srcExpr.getDataType())) {
                        try {
                            targetExpr = targetExpr.castTo(srcExpr.getDataType());
                        } catch (Exception e) {
                            continue;
                        }
                    }

                    boolean isBoundByKeyColumns = true;

                    for (SlotReference slot : targetSlots) {
                        if (!olapScan.getTable().getColumn(slot.getName()).isKey()) {
                            isBoundByKeyColumns = false;
                            break;
                        }
                    }

                    // 根据现在的信息，无法确定isLocalTarget, 默认false
                    RuntimeFilterTarget target = new RuntimeFilterTarget(targetExpr, isBoundByKeyColumns, false);
                    filter.addTarget(target);
                    assign.add(filter);
                }
            }
            return olapScan.addRuntimeFilter(assign);
        }
    }
}
