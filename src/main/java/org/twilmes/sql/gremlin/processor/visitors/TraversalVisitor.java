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
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.processor.TraversalBuilder;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twilmes on 11/29/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class TraversalVisitor implements RelVisitor {

    private static final String PREFIX = "table_";
    private final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap;
    private final Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap;
    @Getter
    private final List<GraphTraversal<?, ?>> traversals = new ArrayList<>();
    @Getter
    private final Map<GremlinToEnumerableConverter, String> tableIdMap = new HashMap<>();
    @Getter
    private final Map<String, GremlinToEnumerableConverter> tableIdConverterMap = new HashMap<>();
    @Getter
    private final Map<String, GraphTraversal<?, ?>> tableTraversalMap = new HashMap<>();
    @Getter
    private final List<Pair<String, String>> joinPairs = new ArrayList<>();
    private Integer id = 0;

    public TraversalVisitor(final GraphTraversalSource traversalSource,
                            final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap,
                            final Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap) {
        this.scanMap = scanMap;
        this.fieldMap = fieldMap;
    }

    @Override
    public void visit(final RelNode node) {
        if (!(node instanceof EnumerableJoin)) {
            return;
        }

        final EnumerableJoin join = (EnumerableJoin) node;
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        final Integer leftKeyId = join.getLeftKeys().get(0);
        final Integer rightKeyId = join.getRightKeys().get(0);

        final List<String> joinFields = join.getRowType().getFieldNames();
        final List<String> leftJoinFields = joinFields.subList(0, left.getRowType().getFieldCount());
        final List<String> rightJoinFields = joinFields.subList(left.getRowType().getFieldCount(), joinFields.size());

        final String leftAliasedColumn = leftJoinFields.get(leftKeyId);
        final String rightAliasedColumn = rightJoinFields.get(rightKeyId);

        if (!(left instanceof GremlinToEnumerableConverter)) {
            // This looks like a bug but isn't.
            left = fieldMap.get(left).get(leftAliasedColumn);
        } else {
            final List<RelNode> scan = scanMap.get(left);
            final GraphTraversal<?, ?> leftTraversal = TraversalBuilder.toTraversal(scan);
            traversals.add(leftTraversal);
            tableTraversalMap.put(getTableId((GremlinToEnumerableConverter) left), leftTraversal);
        }
        if (!(right instanceof GremlinToEnumerableConverter)) {
            right = fieldMap.get(join).get(rightAliasedColumn);
        } else {
            final List<RelNode> scan = scanMap.get(right);
            final GraphTraversal<?, ?> rightTraversal = TraversalBuilder.toTraversal(scan);
            traversals.add(rightTraversal);
            tableTraversalMap.put(getTableId((GremlinToEnumerableConverter) right), rightTraversal);
        }

        final GremlinToEnumerableConverter leftConverter = (GremlinToEnumerableConverter) left;
        final GremlinToEnumerableConverter rightConverter = (GremlinToEnumerableConverter) right;

        final String leftId = getTableId(leftConverter);
        final String rightId = getTableId(rightConverter);

        joinPairs.add(new Pair<>(leftId, rightId));
    }

    private String getTableId(final GremlinToEnumerableConverter converter) {
        String tableId = tableIdMap.get(converter);
        if (tableId == null) {
            tableId = PREFIX + id;
            tableIdMap.put(converter, tableId);
            tableIdConverterMap.put(tableId, converter);
            id++;
        }
        return tableId;
    }
}
