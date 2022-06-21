package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

public class MergeProjectIntoChild extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalProject().then(project -> {
            List<Slot> output = project.getOutput();
            if (output.isEmpty()) {
                return project.child();
            }
            Plan child = project.child();
            List<Slot> childOutput = child.getOutput();
            if (output.equals(childOutput)) {
                return project.child();
            }
            return child.withOutput(output);
        }).toRule(RuleType.MERGE_PROJECTION_INTO_CHILD);
    }
}
