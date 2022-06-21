package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.SlotExtractor;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class PruneJoinColumns extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalProject(logicalJoin()).then(project -> {
            List<Expression> slotExpr = Lists.newArrayList();
            slotExpr.addAll(project.getOutput());
            Set<Slot> joinOutput = Sets.newLinkedHashSet(project.child().getOutput());
            Set<Slot> prunedJoinOutput = Sets.filter(joinOutput, SlotExtractor.extractSlot(slotExpr)::contains);
            if (prunedJoinOutput.size() == joinOutput.size()) {
                return project;
            }

            return project.withChildren(
                    Lists.newArrayList(project.child().withOutput(Lists.newArrayList(prunedJoinOutput))));
        }).toRule(RuleType.COLUMN_PRUNE_JOIN);
    }
}
