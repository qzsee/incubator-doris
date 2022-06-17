package org.apache.doris.nereids.trees.expressions;

import com.google.common.base.Preconditions;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionVisitor;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;

public class CompoundPredicate<LEFT_CHILD_TYPE extends Expression, RIGHT_CHILD_TYPE extends Expression> extends Expression implements BinaryExpression<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>{

    public enum Op {
        AND,
        OR
    }

    private final Op op;

    public CompoundPredicate(Op operator, LEFT_CHILD_TYPE left, RIGHT_CHILD_TYPE right) {
        super(NodeType.COMPOUND, left, right);
        this.op = operator;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCompoundPredicate(this, context);
    }

    public Op getOp() {
        return op;
    }

    public Op opFlip() {
        if (op == Op.AND) {
            return Op.OR;
        }
        if (op == Op.OR) {
            return Op.AND;
        }
        throw new IllegalArgumentException("illegal op!");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundPredicate other = (CompoundPredicate) o;
        return other.getOp() == op && Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, left(), right());
    }

    @Override
    public String sql() {
        return toString();
    }

    @Override
    public String toString() {
        return "(" + left() + " " + op + " " + right() +")";
    }

    @Override
    public CompoundPredicate<Expression, Expression> withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new CompoundPredicate<>(op, children.get(0), children.get(1));
    }
}
