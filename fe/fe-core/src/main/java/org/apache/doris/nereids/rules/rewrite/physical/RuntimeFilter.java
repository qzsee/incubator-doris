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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public final class RuntimeFilter {
    private static final Logger LOG = LogManager.getLogger(RuntimeFilter.class);

    private final RuntimeFilterId id;
    private final Expression srcExpr;
    private final Expression targetExpr;
    private final List<RuntimeFilterTarget> targets = new ArrayList<>();
    private boolean isBroadcastJoin;
    private long ndvEstimate = -1;
    private long filterSizeBytes = 0;
    private boolean hasLocalTargets = false;
    private boolean hasRemoteTargets = false;
    private boolean finalized = false;
    private TRuntimeFilterType runtimeFilterType;
    public static class RuntimeFilterTarget {
        public Expression expr;
        public final boolean isBoundByKeyColumns;
        public final boolean isLocalTarget;

        public RuntimeFilterTarget(Expression targetExpr, boolean isBoundByKeyColumns, boolean isLocalTarget) {
            this.expr = targetExpr;
            this.isBoundByKeyColumns = isBoundByKeyColumns;
            this.isLocalTarget = isLocalTarget;
        }

        @Override
        public String toString() {
            return "expr : " + expr + ", isBoundByKeyColumns : " + isBoundByKeyColumns + ", isLocalTarget : " + isLocalTarget;
        }
    }

    public RuntimeFilter(RuntimeFilterId filterId, Expression srcExpr, Expression origTargetExpr, TRuntimeFilterType type) {
        this.id = filterId;
        this.srcExpr = srcExpr;
        this.targetExpr = origTargetExpr;
        this.runtimeFilterType = type;
    }

    public void addTarget(RuntimeFilterTarget target) {
        this.targets.add(target);
    }

    public Expression getTargetExpr() {
        return targetExpr;
    }

    public Expression getSrcExpr() {
        return srcExpr;
    }

    @Override
    public String toString() {
        return "id : " + id + ", target expr : " + targetExpr + ", src expr : " + srcExpr + ", targets : " + targets;
    }

}
