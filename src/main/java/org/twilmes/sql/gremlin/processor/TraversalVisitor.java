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

import org.apache.calcite.util.Pair;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.schema.SchemaConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twilmes on 11/29/15.
 */
public class TraversalVisitor implements RelVisitor {

    private final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap;
    private final Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap;
    private final SchemaConfig schemaConfig;  // may use to check if join is valid
    private Integer id = 0;
    private Map<GremlinToEnumerableConverter, String> tableIdMap = new HashMap<>();
    private Map<String, GremlinToEnumerableConverter> tableIdConverterMap = new HashMap<>();
    private Map<String, GraphTraversal<?, ?>> tableTraversalMap = new HashMap<>();
    private List<Pair<String, String>> joinPairs = new ArrayList<>();
    private static final String PREFIX = "table_";
    private final List<GraphTraversal> traversals = new ArrayList<>();



    public TraversalVisitor(GraphTraversalSource traversalSource,
                            Map<GremlinToEnumerableConverter, List<RelNode>> scanMap,
                            Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap,
                            SchemaConfig schemaConfig) {
        this.scanMap = scanMap;
        this.fieldMap = fieldMap;
        this.schemaConfig = schemaConfig;
    }

    public List<Pair<String, String>> getJoinPairs() { return joinPairs; }

    public Map<String, GraphTraversal<?, ?>> getTableTraversalMap() { return tableTraversalMap; }

    public Map<String, GremlinToEnumerableConverter> getTableIdConverterMap() { return tableIdConverterMap; }

    @Override
    public void visit(RelNode node) {
        if(!(node instanceof EnumerableJoin)) {
            return;
        }

        final EnumerableJoin join = (EnumerableJoin) node;
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        // TODO: should we check schema to see if the join is valid?
        // found some cases the join happens to two tables that has no relationship

        final Integer leftKeyId = join.getLeftKeys().get(0);
        final Integer rightKeyId = join.getRightKeys().get(0);

        final List<String> joinFields = join.getRowType().getFieldNames();
        final List<String> leftJoinFields = joinFields.subList(0, left.getRowType().getFieldCount());
        final List<String> rightJoinFields = joinFields.subList(left.getRowType().getFieldCount(), joinFields.size());

        final String leftAliasedColumn = leftJoinFields.get(leftKeyId);
        final String rightAliasedColumn = rightJoinFields.get(rightKeyId);

        if(!(left instanceof GremlinToEnumerableConverter)) {
            if (fieldMap.get(left) != null && fieldMap.get(left).containsKey(leftAliasedColumn)) {
                left = fieldMap.get(left).get(leftAliasedColumn);
            } else {
                Map<String, GremlinToEnumerableConverter> leftMap = fieldMap.get(left);
                if (leftMap == null) {
                    for(EnumerableJoin ej : fieldMap.keySet()) {
                        if (ej.getLeft().equals(left)) {
                            left = fieldMap.get(ej).get(leftAliasedColumn);
                            List<RelNode> scan = scanMap.get(left);
                            GraphTraversal leftTraversal = TraversalBuilder.toTraversal(scan);
                            traversals.add(leftTraversal);
                            tableTraversalMap.put(getTableId((GremlinToEnumerableConverter) left), leftTraversal);
                            break;
                        }
                    }
                } else {
                    left = leftMap.get(leftAliasedColumn);
                }
            }
        } else {
            List<RelNode> scan = scanMap.get(left);
            GraphTraversal leftTraversal = TraversalBuilder.toTraversal(scan);
            traversals.add(leftTraversal);
            tableTraversalMap.put(getTableId((GremlinToEnumerableConverter) left), leftTraversal);
        }
        if(!(right instanceof GremlinToEnumerableConverter)) {
            if (fieldMap.get(join) != null && fieldMap.get(join).containsKey(rightAliasedColumn)) {
                right = fieldMap.get(join).get(rightAliasedColumn);
                List<RelNode> scan = scanMap.get(right);
                GraphTraversal rightTraversal = TraversalBuilder.toTraversal(scan);
                traversals.add(rightTraversal);
                tableTraversalMap.put(getTableId((GremlinToEnumerableConverter) right), rightTraversal);
            }
        } else {
            List<RelNode> scan = scanMap.get(right);
            GraphTraversal rightTraversal = TraversalBuilder.toTraversal(scan);
            traversals.add(rightTraversal);
            tableTraversalMap.put(getTableId((GremlinToEnumerableConverter) right), rightTraversal);
        }

        final GremlinToEnumerableConverter leftConverter = (GremlinToEnumerableConverter) left;
        final GremlinToEnumerableConverter rightConverter = (GremlinToEnumerableConverter) right;

        final String leftId = getTableId(leftConverter);
        final String rightId = getTableId(rightConverter);

        joinPairs.add(new Pair<>(leftId, rightId));
    }

    private String getTableId(GremlinToEnumerableConverter converter) {
        String tableId = tableIdMap.get(converter);
        if(tableId == null) {
            tableId = PREFIX + id;
            tableIdMap.put(converter, tableId);
            tableIdConverterMap.put(tableId, converter);
            id++;
        }
        return tableId;
    }

    public Map<GremlinToEnumerableConverter, String> getTableIdMap() {
        return tableIdMap;
    }
}
