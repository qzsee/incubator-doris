package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;

public class PruneAggregationChildrenColumns extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return null;
    }
}
