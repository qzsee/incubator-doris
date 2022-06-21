package org.apache.doris.nereids.util;

import org.apache.doris.nereids.rules.expression.rewrite.ExpressionVisitor;
import org.apache.doris.nereids.trees.expressions.BetweenPredicate;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;

public abstract class IterationVisitor<C> extends ExpressionVisitor<Void, C> {

    public Void visitExpression(Expression expr, C context) {
        return null;
    }

    public Void visitNot(Not expr, C context) {
        process(expr.child(), context);
        return null;
    }

    public Void visitComparisonPredicate(ComparisonPredicate expr, C context) {
        process(expr.left(), context);
        process(expr.right(), context);
        return null;
    }

    public Void visitCompoundPredicate(CompoundPredicate expr, C context) {
        process(expr.left(), context);
        process(expr.right(), context);
        return null;
    }

    public Void visitBetweenPredicate(BetweenPredicate expr, C context) {
        process(expr.getCmp(), context);
        return null;
    }

}
