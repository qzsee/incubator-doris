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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.rewrite.FEFunction;
import org.apache.doris.rewrite.FEFunctionList;
import org.apache.doris.rewrite.FEFunctions;

import com.google.common.collect.ImmutableMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class ExpressionEvaluator {
    private static final Logger LOG = LogManager.getLogger(ExpressionEvaluator.class);

    public static final ExpressionEvaluator INSTANCE = new ExpressionEvaluator();

    public ExpressionEvaluator() {
        registerFunctions();
    }

    private ImmutableMultimap<String, FEFunctionInvoker> functions;

    public Expression evaluate(Expression expression) {

        if (!expression.isConstant()) {
            return expression;
        }

        if (expression instanceof Arithmetic) {

        }
    }


    private FEFunctionInvoker getFunction(FEFunctionSignature signature) {
        Collection<FEFunctionInvoker> functionInvokers = functions.get(signature.getName());
        if (functionInvokers == null) {
            return null;
        }
        for (FEFunctionInvoker invoker : functionInvokers) {
            DataType[] argTypes1 = invoker.getSignature().getArgTypes();
            DataType[] argTypes2 = signature.getArgTypes();

            if (argTypes1.length != argTypes2.length) {
                continue;
            }
            boolean match = true;
            for (int i = 0; i < argTypes1.length; i++) {
                if (!argTypes1[i].equals(argTypes2[i])) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return invoker;
            }
        }
        return null;
    }

    private synchronized void registerFunctions() {
        if (functions != null) {
            return;
        }
        ImmutableMultimap.Builder<String, FEFunctionInvoker> mapBuilder =
                new ImmutableMultimap.Builder<String, FEFunctionInvoker>();
        Class clazz = FEFunctions.class;
        for (Method method : clazz.getDeclaredMethods()) {
            FEFunctionList annotationList = method.getAnnotation(FEFunctionList.class);
            if (annotationList != null) {
                for (FEFunction f : annotationList.value()) {
                    registerFEFunction(mapBuilder, method, f);
                }
            }
            registerFEFunction(mapBuilder, method, method.getAnnotation(FEFunction.class));
        }
        this.functions = mapBuilder.build();
    }

    private void registerFEFunction(ImmutableMultimap.Builder<String, FEFunctionInvoker> mapBuilder,
            Method method, FEFunction annotation) {
        if (annotation != null) {
            String name = annotation.name();
            DataType returnType = DataType.convertFromString(annotation.returnType());
            List<Type> argTypes = new ArrayList<>();
            for (String type : annotation.argTypes()) {
                argTypes.add(ScalarType.createType(type));
            }
            FEFunctionSignature signature = new FEFunctionSignature(name,
                    argTypes.toArray(new DataType[argTypes.size()]), returnType);
            mapBuilder.put(name, new FEFunctionInvoker(method, signature));
        }
    }

    public static class FEFunctionInvoker {
        private final Method method;
        private final FEFunctionSignature signature;

        public FEFunctionInvoker(Method method, FEFunctionSignature signature) {
            this.method = method;
            this.signature = signature;
        }

        public Method getMethod() {
            return method;
        }

        public FEFunctionSignature getSignature() {
            return signature;
        }

        public Literal invoke(List<Literal> args) throws AnalysisException {
            try {
                return (Literal) method.invoke(null, args.toArray());
            } catch (InvocationTargetException | IllegalAccessException | IllegalArgumentException e) {
                throw new AnalysisException(e.getLocalizedMessage());
            }
        }
    }

    public static class FEFunctionSignature {
        private final String name;
        private final DataType[] argTypes;
        private final DataType returnType;

        public FEFunctionSignature(String name, DataType[] argTypes, DataType returnType) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
        }

        public DataType[] getArgTypes() {
            return argTypes;
        }

        public DataType getReturnType() {
            return returnType;
        }

        public String getName() {
            return name;
        }
    }

}
