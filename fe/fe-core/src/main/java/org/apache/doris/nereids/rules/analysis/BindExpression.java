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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Arithmetic;
import org.apache.doris.nereids.trees.expressions.Arithmetic.ArithmeticOperator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.util.TypeUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

public class BindExpression implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.BINDING_FILTER_FUNCTION.build(
                        logicalFilter().then(filter -> {
                            List<Expression> predicates = bind(filter.getExpressions());
                            return new LogicalFilter<>(predicates.get(0), filter.child());
                        })
                )
        );
    }

    private <E extends Expression> List<E> bind(List<E> exprList) {
        return exprList.stream()
                .map(ExpressionBinder.INSTANCE::bind)
                .collect(Collectors.toList());
    }

    private static class ExpressionBinder extends DefaultExpressionRewriter<Void> {
        public static final ExpressionBinder INSTANCE = new ExpressionBinder();

        public <E extends Expression> E bind(E expression) {
            return (E) expression.accept(this, null);
        }

        @Override
        public Expression visitArithmetic(Arithmetic arithmetic, Void context) {
            if (arithmetic.getArithmeticOperator().isBinary()) {
                DataType t1 = TypeUtils.getNumResultType(arithmetic.child(0).getDataType());
                DataType t2 = TypeUtils.getNumResultType(arithmetic.child(1).getDataType());

                DataType commonType = InvalidType.INSTANCE;
                String fnName = arithmetic.getArithmeticOperator().getName();
                ArithmeticOperator operator = arithmetic.getArithmeticOperator();
                switch (operator) {
                    case MULTIPLY:
                    case ADD:
                    case SUBTRACT:
                        commonType = findCommonType(t1, t2);
                        break;
                    case DIVIDE:
                        commonType = findCommonType(t1, t2);
                        if (commonType instanceof BigIntType) {
                            commonType = DoubleType.INSTANCE;
                        }
                        break;
                    default:
                        Preconditions.checkState(false,
                                "Unknown arithmetic operation " + operator.toString() + " in: " + arithmetic.toSql());
                }
                type = castBinaryOp(commonType);
                fn = getBuiltinFunction(fnName, collectChildReturnTypes(), Function.CompareMode.IS_IDENTICAL);
                if (fn == null) {
                    Preconditions.checkState(false, String.format(
                            "No match for '%s' with operand types %s and %s", toSql(), t1, t2));
                }
            }
            return arithmetic;
    }

    private static DataType findCommonType(DataType t1, DataType t2) {

        if (t1 instanceof DoubleType || t2 instanceof DoubleType) {
            return DoubleType.INSTANCE;
        } else {
            if (!(t1 instanceof BigIntType) && !(t2 instanceof BigIntType)) {
                return InvalidType.INSTANCE;
            }
            return BigIntType.INSTANCE;
        }
    }
}
