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

import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.VectorizedUtil;
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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PConstantExprResult;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFoldConstantParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TQueryGlobals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FoldConstantRule extends AbstractExpressionRewriteRule {
    private static final Logger LOG = LogManager.getLogger(FoldConstantRule.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static final FoldConstantRule INSTANCE = new FoldConstantRule();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (ctx.connectContext != null && ctx.connectContext.getSessionVariable().isEnableFoldConstantByBe()) {
            return foldByBe(expr, ctx.connectContext);
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

    private Expression foldByBe(Expression root, ConnectContext context) {
        if (root.isConstant()) {
            Expr expr = ExpressionTranslator.INSTANCE.translate(root, null);

            Map<String, Expr> ori = new HashMap<>();
            ori.put(expr.getId().toString(), expr);

            Map<String, Map<String, TExpr>> paramMap = new HashMap<>();
            Map<String, TExpr> constMap = new HashMap<>();

            collectConstExpr(expr, constMap);
            paramMap.put("0", constMap);
            Map<String, Map<String, Expr>> res = calc(paramMap, ori, context);
            System.out.println(res);
            return null;
        }
        return root;
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

    private Map<String, Map<String, Expr>> calc(Map<String, Map<String, TExpr>> map, Map<String, Expr> allConstMap,
            ConnectContext context) {
        TNetworkAddress brpcAddress = null;
        Map<String, Map<String, Expr>> resultMap = new HashMap<>();
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new LoadException("Failed to get all partitions. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            brpcAddress = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            TQueryGlobals queryGlobals = new TQueryGlobals();
            queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
            queryGlobals.setTimestampMs(System.currentTimeMillis());
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            if (context.getSessionVariable().getTimeZone().equals("CST")) {
                queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            } else {
                queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
            }

            TFoldConstantParams tParams = new TFoldConstantParams(map, queryGlobals);
            tParams.setVecExec(VectorizedUtil.isVectorized());

            Future<PConstantExprResult> future
                    = BackendServiceProxy.getInstance().foldConstantExpr(brpcAddress, tParams);
            InternalService.PConstantExprResult result = future.get(5, TimeUnit.SECONDS);

            if (result.getStatus().getStatusCode() == 0) {
                for (Map.Entry<String, InternalService.PExprResultMap> entry
                        : result.getExprResultMapMap().entrySet()) {
                    Map<String, Expr> tmp = new HashMap<>();
                    for (Map.Entry<String, InternalService.PExprResult> entry1
                            : entry.getValue().getMapMap().entrySet()) {
                        TPrimitiveType type = TPrimitiveType.findByValue(entry1.getValue().getType().getType());
                        Expr retExpr = null;
                        if (entry1.getValue().getSuccess()) {
                            retExpr = LiteralExpr.create(entry1.getValue().getContent(),
                                    Type.fromPrimitiveType(PrimitiveType.fromThrift(type)));
                        } else {
                            retExpr = allConstMap.get(entry1.getKey());
                        }
                        tmp.put(entry1.getKey(), retExpr);
                    }
                    if (!tmp.isEmpty()) {
                        resultMap.put(entry.getKey(), tmp);
                    }
                }

            } else {
                LOG.warn("failed to get const expr value from be: {}", result.getStatus().getErrorMsgsList());
            }
        } catch (Exception e) {
            LOG.warn("failed to get const expr value from be: {}", e.getMessage());
        }
        return resultMap;
    }
}
