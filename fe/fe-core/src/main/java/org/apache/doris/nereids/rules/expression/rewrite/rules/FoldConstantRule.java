// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullLiteral;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FoldConstantRule extends AbstractExpressionRewriteRule {

    public static final FoldConstantRule INSTANCE = new FoldConstantRule();

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        Expression child = rewrite(not.child(), context);
        if (child.isLiteral()) {
            if (child instanceof BooleanLiteral) {
                BooleanLiteral c = (BooleanLiteral) child;
                return BooleanLiteral.of(!c.getValue());
            }
        }
        return not;
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        if (!(cp instanceof NullSafeEqual) && hasNullChildren(cp.children())) {
            return new NullLiteral();
        }

        Expression left = rewrite(cp.left(), context);
        Expression right = rewrite(cp.right(), context);

        if (isLiteralChildren(left, right)) {
            Literal l = (Literal) left;
            Literal r = (Literal) right;
            if (cp instanceof EqualTo) {
                return BooleanLiteral.of(l.compareLiteral(r) == 0);
            }
            if (cp instanceof GreaterThan) {
                return BooleanLiteral.of(l.compareLiteral(r) > 0);
            }
            if (cp instanceof GreaterThanEqual) {
                return BooleanLiteral.of(l.compareLiteral(r) >= 0);
            }
            if (cp instanceof LessThan) {
                return BooleanLiteral.of(l.compareLiteral(r) < 0);
            }
            if (cp instanceof LessThanEqual) {
                return BooleanLiteral.of(l.compareLiteral(r) <= 0);
            }
            if (cp instanceof NullSafeEqual) {
                if (l.isNull() && r.isNull()) {
                    return BooleanLiteral.TRUE;
                } else if (!l.isNull() && !r.isNull()) {
                    return BooleanLiteral.of(l.compareLiteral(r) == 0);
                } else {
                    return BooleanLiteral.FALSE;
                }
            }
        }
        return cp;
    }

    @Override
    public Expression visitArithmetic(Arithmetic arithmetic, ExpressionRewriteContext context) {
        if (arithmetic.children().size() == 2) {
            Expression left = rewrite(arithmetic.child(0), context);
            Expression right = rewrite(arithmetic.child(1), context);
            if (isLiteralChildren(left, right)) {
                return ExpressionEvaluator.INSTANCE.evaluate(arithmetic.withChildren(left, right));
            }
        }
        return arithmetic;
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        List<Expression> newArgs = boundFunction.getArguments().stream().map(arg -> rewrite(arg, context))
                .collect(Collectors.toList());
        if (isLiteralChildren(newArgs)) {
            return ExpressionEvaluator.INSTANCE.evaluate(boundFunction.withChildren(newArgs));
        }
        return boundFunction;
    }

    private static boolean isLiteralChildren(Expression... children) {
        return Arrays.stream(children).allMatch(c -> c instanceof Literal);
    }

    private static boolean isLiteralChildren(List<Expression> children) {
        return children.stream().allMatch(c -> c instanceof Literal);
    }

    private static boolean hasNullChildren(List<Expression> children) {
        return children.stream().anyMatch(c -> c instanceof NullLiteral);
    }

}
