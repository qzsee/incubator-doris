package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.rules.expression.rewrite.ExpressionVisitor;
import org.apache.doris.nereids.trees.NodeType;

import java.util.Objects;


public class BetweenPredicate<CMP_CHILD_TYPE extends Expression, LOWER_CHILD_TYPE extends Expression, UPPER_CHILD_TYPE extends Expression>
        extends Expression<BetweenPredicate<CMP_CHILD_TYPE, LOWER_CHILD_TYPE, UPPER_CHILD_TYPE>> {

    private final Expression cmp;
    private final Expression lower;
    private final Expression upper;

    public BetweenPredicate(Expression cmp, Expression lower, Expression upper) {
        super(NodeType.BETWEEN, cmp, lower, upper);
        this.cmp = cmp;
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    public Expression getCmp() {
        return cmp;
    }

    public Expression getLower() {
        return lower;
    }

    public Expression getUpper() {
        return upper;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BetweenPredicate that = (BetweenPredicate) o;
        return Objects.equals(cmp, that.cmp) && Objects.equals(lower, that.lower) && Objects.equals(upper, that.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cmp, lower, upper);
    }

}
