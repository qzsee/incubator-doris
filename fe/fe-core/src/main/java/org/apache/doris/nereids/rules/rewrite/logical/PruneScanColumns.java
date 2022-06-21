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

public class PruneScanColumns extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalProject(logicalRelation()).then(project -> {
            List<Expression> slotExpr = Lists.newArrayList();
            slotExpr.addAll(project.getOutput());
            Set<Slot> scanOutput = Sets.newLinkedHashSet(project.child().getOutput());
            Set<Slot> prunedScanOutput = Sets.filter(scanOutput, SlotExtractor.extractSlot(slotExpr)::contains);
            if (prunedScanOutput.size() == scanOutput.size()) {
                return project;
            }

            return project.withChildren(
                    Lists.newArrayList(project.child().withOutput(Lists.newArrayList(prunedScanOutput))));
        }).toRule(RuleType.COLUMN_PRUNE_SCAN);
    }
}
