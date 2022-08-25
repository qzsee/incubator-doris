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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InformationFunction;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SysVariableDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.thrift.TExpr;

import com.google.common.base.Predicates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FoldConstantRule extends AbstractExpressionRewriteRule {

    public static final FoldConstantRule INSTANCE = new FoldConstantRule();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (ctx.connectContext.getSessionVariable().isEnableFoldConstantByBe()) {
            return foldByBe(expr);
        }
        return super.rewrite(expr, ctx);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate compoundPredicate, ExpressionRewriteContext context) {
        Expression left = rewrite(compoundPredicate.left(), context);
        Expression right = rewrite(compoundPredicate.right(), context);
        if (left instanceof NullLiteral && right instanceof NullLiteral) {
            return Literal.of(null);
        }
        if (left instanceof Literal || right instanceof Literal) {
            if (compoundPredicate instanceof And) {
                if (left.equals(BooleanLiteral.FALSE) || right.equals(BooleanLiteral.FALSE)) {
                    return BooleanLiteral.FALSE;
                }
                if (left.equals(BooleanLiteral.TRUE)) {
                    return right;
                }
                if (right.equals(BooleanLiteral.TRUE)) {
                    return left;
                }
            }
            if (compoundPredicate instanceof Or) {
                if (left.equals(BooleanLiteral.TRUE) || right.equals(BooleanLiteral.TRUE)) {
                    return BooleanLiteral.TRUE;
                }
                if (left.equals(BooleanLiteral.FALSE)) {
                    return right;
                }
                if (right.equals(BooleanLiteral.FALSE)) {
                    return left;
                }
            }
        }
        return compoundPredicate.withChildren(left, right);
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
        Expression newDefault = null;
        boolean foundNewDefault = false;

        List<WhenClause> whenClauses = new ArrayList<>();
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            Expression whenOperand = rewrite(whenClause.getOperand(), context);

            if (!(whenOperand instanceof Literal)) {
                whenClauses.add(new WhenClause(whenOperand, rewrite(whenClause.getResult(), context)));
            } else if (BooleanLiteral.TRUE.equals(whenOperand)) {
                foundNewDefault = true;
                newDefault = rewrite(whenClause.getResult(), context);
                break;
            }
        }

        Expression defaultResult;
        if (foundNewDefault) {
            defaultResult = newDefault;
        } else {
            defaultResult = rewrite(caseWhen.getDefaultValue().orElse(Literal.of(null)), context);
        }

        if (whenClauses.isEmpty()) {
            return defaultResult;
        }
        return new CaseWhen(whenClauses, defaultResult);
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, ExpressionRewriteContext context) {
        List<Expression> newChildren = inPredicate.children().stream().map(c -> rewrite(c, context))
                .collect(Collectors.toList());
        if (newChildren.get(0) instanceof NullLiteral) {
            return Literal.of(null);
        }
        if (isAllLiteral(newChildren)) {
            Literal c0 = (Literal) newChildren.get(0);
            for (int i = 1; i < newChildren.size(); i++) {
                if (c0.compareTo((Literal) newChildren.get(i)) == 0) {
                    return Literal.of(true);
                }
            }
            return Literal.of(false);
        }
        return inPredicate.withChildren(newChildren);
    }

    @Override
    public Expression visitIsNull(IsNull isNull, ExpressionRewriteContext context) {
        Expression child = rewrite(isNull.child(), context);
        if (child instanceof NullLiteral) {
            return Literal.of(true);
        } else if (child instanceof Literal) {
            return Literal.of(false);
        }
        return isNull.withChildren(child);
    }

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        if (hasNull(cast.children())) {
            return Literal.of(null);
        }
        Expression child = rewrite(cast.child(), context);
        if (child.isLiteral()) {
            return child.castTo(cast.getDataType());
        }
        return cast;
    }

    @Override
    public Expression visitNot(Not not, ExpressionRewriteContext context) {
        Expression child = rewrite(not.child(), context);
        if (child instanceof NullLiteral) {
            return Literal.of(null);
        }
        if (child.isLiteral()) {
            if (child instanceof BooleanLiteral) {
                BooleanLiteral c = (BooleanLiteral) child;
                return BooleanLiteral.of(!c.getValue());
            }
        }
        return not.withChildren(child);
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        Expression left = rewrite(cp.left(), context);
        Expression right = rewrite(cp.right(), context);

        if (!(cp instanceof NullSafeEqual) && hasNull(left, right)) {
            return Literal.of(null);
        }

        if (isAllLiteral(left, right)) {
            Literal l = (Literal) left;
            Literal r = (Literal) right;
            if (cp instanceof EqualTo) {
                return BooleanLiteral.of(l.compareTo(r) == 0);
            }
            if (cp instanceof GreaterThan) {
                return BooleanLiteral.of(l.compareTo(r) > 0);
            }
            if (cp instanceof GreaterThanEqual) {
                return BooleanLiteral.of(l.compareTo(r) >= 0);
            }
            if (cp instanceof LessThan) {
                return BooleanLiteral.of(l.compareTo(r) < 0);
            }
            if (cp instanceof LessThanEqual) {
                return BooleanLiteral.of(l.compareTo(r) <= 0);
            }
            if (cp instanceof NullSafeEqual) {
                if (l.isNull() && r.isNull()) {
                    return BooleanLiteral.TRUE;
                } else if (!l.isNull() && !r.isNull()) {
                    return BooleanLiteral.of(l.compareTo(r) == 0);
                } else {
                    return BooleanLiteral.FALSE;
                }
            }
        }
        return cp.withChildren(left, right);
    }

    @Override
    public Expression visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, ExpressionRewriteContext context) {
        Expression left = rewrite(binaryArithmetic.left(), context);
        Expression right = rewrite(binaryArithmetic.right(), context);
        if (left instanceof NullLiteral || right instanceof NullLiteral) {
            return Literal.of(null);
        }
        if (isAllLiteral(left, right)) {
            return ExpressionEvaluator.INSTANCE.eval(binaryArithmetic.withChildren(left, right));
        }
        return binaryArithmetic.withChildren(left, right);
    }

    @Override
    public Expression visitBoundFunction(BoundFunction boundFunction, ExpressionRewriteContext context) {
        List<Expression> newArgs = boundFunction.getArguments().stream().map(arg -> rewrite(arg, context))
                .collect(Collectors.toList());
        if (isAllLiteral(newArgs)) {
            return ExpressionEvaluator.INSTANCE.eval(boundFunction.withChildren(newArgs));
        }
        return boundFunction.withChildren(newArgs);
    }

    @Override
    public Expression visitTimestampArithmetic(TimestampArithmetic arithmetic, ExpressionRewriteContext context) {
        Expression left = rewrite(arithmetic.child(0), context);
        Expression right = rewrite(arithmetic.child(1), context);
        if (isAllLiteral(left, right)) {
            return ExpressionEvaluator.INSTANCE.eval(arithmetic.withChildren(left, right));
        }
        return arithmetic.withChildren(left, right);
    }

    private static boolean isAllLiteral(Expression... children) {
        return Arrays.stream(children).allMatch(c -> c instanceof Literal);
    }

    private static boolean isAllLiteral(List<Expression> children) {
        return children.stream().allMatch(c -> c instanceof Literal);
    }

    private static boolean hasNull(List<Expression> children) {
        return children.stream().anyMatch(c -> c instanceof NullLiteral);
    }

    private static boolean hasNull(Expression... children) {
        return Arrays.stream(children).anyMatch(c -> c instanceof NullLiteral);
    }

    private Expression foldByBe(Expression root) {
        Expr expr = ExpressionTranslator.INSTANCE.translate(root, null);

    }

    private void collectConstExpr(Expr expr, Map<String, TExpr> constExprMap) {
        if (expr.isConstant()) {
            if (expr instanceof CastExpr) {
                CastExpr castExpr = (CastExpr) expr;
                if (castExpr.getChild(0) instanceof org.apache.doris.analysis.NullLiteral) {
                    return;
                }
            }
            if (expr instanceof LiteralExpr) {
                return;
            }
            if (expr instanceof BetweenPredicate) {
                return;
            }
            constExprMap.put(expr.getId().toString(), expr.treeToThrift());
        }
        for (int i = 0; i < expr.getChildren().size(); i++) {
            final Expr child = expr.getChildren().get(i);
            collectConstExpr(child, constExprMap);
        }
    }
}
