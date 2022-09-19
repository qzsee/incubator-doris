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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * derive additional predicates.
 * for example:
 * a = b and a = 1 => b = 1
 */
public class PredicatePropagation {

    /**
     * infer additional predicates.
     */
    public Set<Expression> infer(List<Expression> predicates) {
        Set<Expression> inferred = Sets.newHashSet();
        for (Expression predicate : predicates) {
            if (canEquivalentDeduce(predicate)) {
                List<Expression> candidates = subtract(predicates, predicate);
                candidates.forEach(candidate -> {
                    inferred.add(transform(candidate, predicate.child(0), predicate.child(1)));
                });
            }
        }
        predicates.forEach(inferred::remove);
        return inferred;
    }

    private Expression transform(Expression expression, Expression source, Expression target) {
        Expression rewritten = replace(expression, source, target);
        if (expression.equals(rewritten)) {
            rewritten = mapChildren(expression, source, target);
            if (expression.equals(rewritten)) {
                return expression;
            } else {
                return rewritten;
            }
        } else {
            return mapChildren(rewritten, source, target);
        }
    }

    private Expression mapChildren(Expression expression, Expression source, Expression target) {
        if (!expression.children().isEmpty()) {
            List<Expression> children = Lists.newArrayList();
            for (Expression child : expression.children()) {
                children.add(transform(child, source, target));
            }
            return expression.withChildren(children);
        } else {
            return expression;
        }
    }

    private Expression replace(Expression expression, Expression source, Expression target) {
        if (expression.equals(source)) {
            return target;
        }
        if (expression.equals(target)) {
            return source;
        }
        return expression;
    }

    private boolean canEquivalentDeduce(Expression predicate) {
        return predicate instanceof EqualTo && predicate.children().stream().allMatch(e -> e instanceof SlotReference);
    }

    private List<Expression> subtract(List<Expression> expressions, Expression target) {
        ArrayList<Expression> cloneList = Lists.newArrayList(expressions);
        cloneList.remove(target);
        return cloneList;
    }
}
