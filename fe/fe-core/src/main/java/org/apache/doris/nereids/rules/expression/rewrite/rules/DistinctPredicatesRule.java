package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rewrite.RewriteHelper;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.clearspring.analytics.util.Lists;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class DistinctPredicatesRule extends AbstractExpressionRewriteRule {

    public static final DistinctPredicatesRule INSTANCE = new DistinctPredicatesRule();

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate expr, ExpressionRewriteContext context) {
        List<Expression> extractExpressions = RewriteHelper.extract(expr);
        Set<Expression> distinctExpressions = new LinkedHashSet<>(extractExpressions);
        if (distinctExpressions.size() != extractExpressions.size()) {
            return RewriteHelper.compound(expr.getOp(), Lists.newArrayList(distinctExpressions));
        }
        return expr;
    }
}
