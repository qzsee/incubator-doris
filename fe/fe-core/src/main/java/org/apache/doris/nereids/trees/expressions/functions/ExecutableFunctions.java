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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.IntegerLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.rewrite.FEFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

public class ExecutableFunctions {
    private static final Logger LOG = LogManager.getLogger(ExecutableFunctions.class);

    public static final ExecutableFunctions INSTANCE = new ExecutableFunctions();

    /**
     * Executable arithmetic functions
     */

    @FEFunction(name = "add", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static IntegerLiteral addTinyint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, SmallIntType.INSTANCE);
    }

    @FEFunction(name = "add", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral addSmallint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, IntegerType.INSTANCE);
    }

    @FEFunction(name = "add", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static IntegerLiteral addInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, BigIntType.INSTANCE);
    }

    @FEFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntegerLiteral addBigint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.addExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, BigIntType.INSTANCE);
    }

    @FEFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral addDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() + second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral addDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().add(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static IntegerLiteral subtractTinyint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, SmallIntType.INSTANCE);
    }

    @FEFunction(name = "subtract", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral subtractSmallint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static IntegerLiteral subtractInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, BigIntType.INSTANCE);
    }

    @FEFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntegerLiteral subtractBigint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.subtractExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, BigIntType.INSTANCE);
    }

    @FEFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral subtractDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() - second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral subtractDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().subtract(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"TINYINT", "TINYINT"}, returnType = "SMALLINT")
    public static IntegerLiteral multiplyTinyint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, SmallIntType.INSTANCE);
    }

    @FEFunction(name = "multiply", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "INT")
    public static IntegerLiteral multiplySmallint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"INT", "INT"}, returnType = "BIGINT")
    public static IntegerLiteral multiplyInt(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, BigIntType.INSTANCE);
    }

    @FEFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntegerLiteral multiplyBigint(IntegerLiteral first, IntegerLiteral second) {
        long result = Math.multiplyExact(first.getValue(), second.getValue());
        return new IntegerLiteral(result, BigIntType.INSTANCE);
    }

    @FEFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral multiplyDouble(DoubleLiteral first, DoubleLiteral second) {
        double result = first.getValue() * second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral multiplyDecimal(DecimalLiteral first, DecimalLiteral second) {
        BigDecimal result = first.getValue().multiply(second.getValue());
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static DoubleLiteral divideDouble(DoubleLiteral first, DoubleLiteral second) {
        if (second.getValue() == 0.0) {
            return null;
        }
        double result = first.getValue() / second.getValue();
        return new DoubleLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral divideDecimal(DecimalLiteral first, DecimalLiteral second) {
        if (first.getValue().compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal result = first.getValue().divide(second.getValue());
        return new DecimalLiteral(result);
    }
}
