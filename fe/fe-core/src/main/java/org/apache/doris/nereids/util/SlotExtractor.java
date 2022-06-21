package org.apache.doris.nereids.util;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

public class SlotExtractor extends IterationVisitor<List<Slot>> {

    public static Set<Slot> extractSlot(List<Expression> expressions) {

        Set<Slot> slots = Sets.newLinkedHashSet();
        for (Expression expression : expressions) {
            slots.addAll(extractSlot(expression));
        }
        return slots;
    }

    public static List<Slot> extractSlot(Expression expression) {
        List<Slot> slots = Lists.newArrayList();
        new SlotExtractor().process(expression, slots);
        return slots;
    }

    @Override
    public Void visitSlot(Slot expr, List<Slot> context) {
        context.add(expr);
        return null;
    }
}
