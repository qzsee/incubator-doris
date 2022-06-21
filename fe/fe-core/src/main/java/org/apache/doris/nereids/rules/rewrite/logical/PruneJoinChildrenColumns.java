package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.SlotExtractor;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PruneJoinChildrenColumns extends OneRewriteRuleFactory {
    @Override
    public Rule<Plan> build() {
        logicalJoin().then(join -> {
            Set<Slot> joinOutputs = Sets.newLinkedHashSet(join.getOutput());
            joinOutputs.addAll(SlotExtractor.extractSlot(join.operator.getOnClause().get()));
            List<Slot> leftOutputs = join.left().getOutput().stream().filter(joinOutputs::contains)
                    .collect(Collectors.toList());
            List<Slot> rightOutputs = join.right().getOutput().stream().filter(joinOutputs::contains)
                    .collect(Collectors.toList());
            if (leftOutputs.size() != join.left().getOutput().size()) {
                join.withChildren(Lists.newArrayList(join.left().withOutput(leftOutputs), join.right()));
            }
            if (rightOutputs.size() != join.right().getOutput().size()) {
                join.withChildren(Lists.newArrayList(join.left(), join.right().withOutput(rightOutputs)));
            }
            return join;
        }).toRule(RuleType.COLUMN_PRUNE_JOIN_CHILDREN);
        return null;
    }
}
