/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.processor.visitors;

import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.rel.RelNode;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a mapping from field/column name to GremlinEnumerableConverter for each join.
 * This lets us track column name lineage back to the table it came from.
 * <p>
 * Created by twilmes on 11/29/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class FieldMapVisitor implements RelVisitor {
    @Getter
    private final Map<EnumerableHashJoin, Map<String, GremlinToEnumerableConverter>> fieldMap = new HashMap<>();

    @Override
    public void visit(final RelNode node) {
        // we only care about joins
        if (!(node instanceof EnumerableHashJoin)) {
            return;
        }
        final EnumerableHashJoin join = (EnumerableHashJoin) node;
        final RelNode left = join.getLeft();
        final RelNode right = join.getRight();

        if (!fieldMap.containsKey(join)) {
            fieldMap.put(join, new HashMap<>());
        }

        final List<String> leftFields = join.getRowType().getFieldNames().
                subList(0, left.getRowType().getFieldCount());
        if (left instanceof GremlinToEnumerableConverter) {
            leftFields.forEach(field -> fieldMap.get(join).put(field, (GremlinToEnumerableConverter) left));
        } else if (left instanceof EnumerableHashJoin) {
            // we still need to figure out these fields...so walk on down
            int col = 0;
            for (final String field : leftFields) {
                fieldMap.get(join).put(field, getConverter(col, field, left));
                col++;
            }
        } else if (left instanceof EnumerableCalc) {
            if (left.getInput(0) instanceof GremlinToEnumerableConverter) {
                leftFields.forEach(
                        field -> fieldMap.get(join).put(field, (GremlinToEnumerableConverter) left.getInput(0)));
            }
        }

        final List<String> rightFields = join.getRowType().getFieldNames().
                subList(left.getRowType().getFieldCount(), join.getRowType().getFieldCount());
        if (right instanceof GremlinToEnumerableConverter) {
            rightFields.forEach(field -> fieldMap.get(join).put(field, (GremlinToEnumerableConverter) right));
        } else if (right instanceof EnumerableHashJoin) {
            // we still need to figure out these fields...so walk on down
            int col = 0;
            for (final String field : rightFields) {
                fieldMap.get(join).put(field, getConverter(col, field, right));
                col++;
            }
        } else if (right instanceof EnumerableCalc) {
            if (right.getInput(0) instanceof GremlinToEnumerableConverter) {
                leftFields.forEach(
                        field -> fieldMap.get(join).put(field, (GremlinToEnumerableConverter) right.getInput(0)));
            }
        }
    }

    private GremlinToEnumerableConverter getConverter(final int fieldIndex, final String field, final RelNode node) {
        if (node instanceof EnumerableHashJoin) {
            final EnumerableHashJoin join = (EnumerableHashJoin) node;
            final List<String> fieldNames = join.getRowType().getFieldNames();
            final String chosenField = fieldNames.get(fieldIndex);
            return fieldMap.get(join).get(chosenField);
        } else if (node instanceof EnumerableCalc) {
            final EnumerableCalc calc = (EnumerableCalc) node;
            final String chosenField = calc.getRowType().getFieldNames().get(fieldIndex);
            return fieldMap.get(node).get(chosenField);
        }
        return null;
    }
}
