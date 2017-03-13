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

package org.twilmes.sql.gremlin.processor;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a mapping from field/column name to GremlinEnumerableConverter for each join.
 * This lets us track column name lineage back to the table it came from.
 *
 * Created by twilmes on 11/29/15.
 */
public class FieldMapVisitor implements RelVisitor {
    private final Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap = new HashMap<>();

    public Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> getFieldMap() {
        return fieldMap;
    }

    @Override
    public void visit(RelNode node) {
        // we only care about joins
        if(!(node instanceof EnumerableJoin)) {
            return;
        }
        final EnumerableJoin join = (EnumerableJoin) node;
        final RelNode left = join.getLeft();
        final RelNode right = join.getRight();

        if(!fieldMap.containsKey(join)) {
            fieldMap.put(join, new HashMap<>());
        }

        if(left instanceof GremlinToEnumerableConverter) {
            final List<String> leftFields = join.getRowType().getFieldNames().
                    subList(0, left.getRowType().getFieldCount());
            leftFields.stream().forEach(field -> {
                fieldMap.get(join).put(field, (GremlinToEnumerableConverter) left);
            });
        } else {
            // we still need to figure out these fields...so walk on down
            final List<String> leftFields = join.getRowType().getFieldNames().
                    subList(0, left.getRowType().getFieldCount());
            int col = 0;
            for(String field : leftFields) {
                fieldMap.get(join).put(field, getConverter(col, field, left));
                col++;
            }
        }

        if(right instanceof GremlinToEnumerableConverter) {
            final List<String> rightFields = join.getRowType().getFieldNames().
                    subList(left.getRowType().getFieldCount(), join.getRowType().getFieldCount());
            rightFields.stream().forEach(field -> {
                fieldMap.get(join).put(field, (GremlinToEnumerableConverter) right);
            });
        } else {
            // we still need to figure out these fields...so walk on down
            final List<String> rightFields = join.getRowType().getFieldNames().
                    subList(left.getRowType().getFieldCount(), join.getRowType().getFieldCount());
            int col = 0;
            for(String field : rightFields) {
                fieldMap.get(join).put(field, getConverter(col, field, right));
                col++;
            }
        }
    }

    private GremlinToEnumerableConverter getConverter(int fieldIndex, String field, RelNode node) {
        if(node instanceof EnumerableJoin) {
            final EnumerableJoin join = (EnumerableJoin) node;
            List<String> fieldNames = join.getRowType().getFieldNames();
            final String chosenField = fieldNames.get(fieldIndex);
            return fieldMap.get(join).get(chosenField);
        } else if (node instanceof EnumerableCalc) {
            final EnumerableCalc calc = (EnumerableCalc) node;
            for (RelNode relNode : calc.getInputs()) {
                if (relNode instanceof EnumerableJoin) {
                    return getConverter(fieldIndex, field, relNode);
                } else {
                    try {
                        GremlinToEnumerableConverter converter = (GremlinToEnumerableConverter) relNode;
                        return converter;
                    } catch (ClassCastException e) {

                    }
                }
            }
        }
        return null;
    }
}
