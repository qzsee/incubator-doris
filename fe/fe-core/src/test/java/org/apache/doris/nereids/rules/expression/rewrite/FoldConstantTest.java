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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rewrite.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rewrite.rules.TypeCoercion;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.TimestampArithmetic;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class FoldConstantTest {

    private static final NereidsParser PARSER = new NereidsParser();
    private ExpressionRuleExecutor executor;

    @Test
    public void testCaseWhenFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("case when 1 = 2 then 1 when '1' < 2 then 2 else 3 end", "2");
        assertRewrite("case when 1 = 2 then 1 when '1' > 2 then 2 end", "null");
        assertRewrite("case when (1 + 5) / 2 > 2 then 4  when '1' < 2 then 2 else 3 end", "4");
        assertRewrite("case when not 1 = 2 then 1 when '1' > 2 then 2 end", "1");
        assertRewrite("case when 1 = 2 then 1 when 3 in ('1',2 + 8 / 2,3,4) then 2 end", "2");
    }

    @Test
    public void testInFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("1 in (1,2,3,4)", "true");
        // Type Coercion trans all to string.
        assertRewrite("3 in ('1',2 + 8 / 2,3,4)", "true");
        assertRewrite("null in ('1',2 + 8 / 2,3,4)", "null");
    }

    @Test
    public void testLogicalFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("10 + 1 > 1 and 1 > 2", "false");
        assertRewrite("10 + 1 > 1 and 1 < 2", "true");
        assertRewrite("null + 1 > 1 and 1 < 2", "null");
        assertRewrite("10 < 3 and 1 > 2", "false");
        assertRewrite("6 / 2 - 10 * (6 + 1) > 2 and 10 > 3 and 1 > 2", "false");

        assertRewrite("10 + 1 > 1 or 1 > 2", "true");
        assertRewrite("null + 1 > 1 or 1 > 2", "null");
        assertRewrite("6 / 2 - 10 * (6 + 1) > 2 or 10 > 3 or 1 > 2", "true");

        assertRewrite("(1 > 5 and 8 < 10 or 1 = 3) or (1 > 8 + 9 / (10 * 2) or ( 10 = 3))", "false");
    }

    @Test
    public void testIsNullFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("100 is null", "false");
        assertRewrite("null is null", "true");
        assertRewrite("100 is not null", "true");
    }

    @Test
    public void testNotFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("not 1 > 2", "true");
        assertRewrite("not null + 1 > 2", "null");
        assertRewrite("not (1 + 5) / 2 + (10 - 1) * 3 > 3 * 5 + 1", "false");
    }

    @Test
    public void testCastFold() {
        executor = new ExpressionRuleExecutor(FoldConstantRule.INSTANCE);
        // cast '1' as tinyint
        Cast c = new Cast(Literal.of("1"), TinyIntType.INSTANCE);
        Expression rewritten = executor.rewrite(c);
        Literal expected = Literal.of((byte) 1);
        Assertions.assertEquals(rewritten, expected);
    }

    @Test
    public void testCompareFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("'1' = 2", "false");
        assertRewrite("1 = 2", "false");
        assertRewrite("1 != 2", "true");
        assertRewrite("2 > 2", "false");
        assertRewrite("3 * 10 + 1 / 2 >= 2", "true");
        assertRewrite("3 < 2", "false");
        assertRewrite("3 <= 2", "false");
        assertRewrite("3 <= null", "null");
        assertRewrite("3 >= null", "null");
        assertRewrite("null <=> null", "true");
        assertRewrite("2 <=> null", "false");
        assertRewrite("2 <=> 2", "true");
    }

    @Test
    public void testArithmeticFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        assertRewrite("1 + 1", Literal.of(2L));
        assertRewrite("1 - 1", Literal.of(0L));
        assertRewrite("100 + 100", Literal.of(200L));
        assertRewrite("1 - 2", Literal.of(-1L));

        assertRewrite("1 - 2 > 1", "false");
        assertRewrite("1 - 2 + 1 > 1 + 1 - 100", "true");
        assertRewrite("10 * 2 / 1 + 1 > (1 + 1) - 100", "true");

        // a + 1 > 2
        Slot a = SlotReference.of("a", IntegerType.INSTANCE);
        Expression e1 = new Add(a, Literal.of(1L));
        Expression e2 = new Add(new Cast(a, BigIntType.INSTANCE), Literal.of(1L));
        assertRewrite(e1, e2);

        // a > (1 + 10) / 2 * (10 + 1)
        Expression e3 = PARSER.parseExpression("(1 + 10) / 2 * (10 + 1)");
        Expression e4 = new GreaterThan(a, e3);
        Expression e5 = new GreaterThan(new Cast(a, DoubleType.INSTANCE), Literal.of(60.5D));
        assertRewrite(e4, e5);

        // a > 1
        Expression e6 = new GreaterThan(a, Literal.of(1));
        assertRewrite(e6, e6);
        assertRewrite(a, a);

        // a
        assertRewrite(a, a);

        // 1
        Literal one = Literal.of(1);
        assertRewrite(one, one);
    }

    @Test
    public void testTimestampFold() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(TypeCoercion.INSTANCE, FoldConstantRule.INSTANCE));
        String interval = "'1991-05-01' - interval 1 day";
        Expression e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        Expression e8 = new DateTimeLiteral(1991, 4, 30, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval '1' day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 2, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "'1991-05-01' + interval 1+1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 3, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "date '1991-05-01' + interval 10 / 2 + 1 day";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 7, 0, 0, 0);
        assertRewrite(e7, e8);

        interval = "interval '1' day + '1991-05-01'";
        e7 = process((TimestampArithmetic) PARSER.parseExpression(interval));
        e8 = new DateTimeLiteral(1991, 5, 2, 0, 0, 0);
        assertRewrite(e7, e8);
    }

    public Expression process(TimestampArithmetic arithmetic) {
        String funcOpName;
        if (arithmetic.getFuncName() == null) {
            funcOpName = String.format("%sS_%s", arithmetic.getTimeUnit(),
                    (arithmetic.getOp() == Operator.ADD) ? "ADD" : "SUB");
        } else {
            funcOpName = arithmetic.getFuncName();
        }
        return arithmetic.withFuncName(funcOpName.toLowerCase(Locale.ROOT));
    }


    private void assertRewrite(String expression, String expected) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private void assertRewrite(String expression, Expression expectedExpression) {
        Expression needRewriteExpression = PARSER.parseExpression(expression);
        Expression rewrittenExpression = executor.rewrite(needRewriteExpression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }

    private void assertRewrite(Expression expression, Expression expectedExpression) {
        Expression rewrittenExpression = executor.rewrite(expression);
        Assertions.assertEquals(expectedExpression, rewrittenExpression);
    }
}
