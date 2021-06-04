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

import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.twilmes.sql.gremlin.ParseException;
import org.twilmes.sql.gremlin.rel.GremlinFilter;
import org.twilmes.sql.gremlin.rel.GremlinTableScan;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.schema.LabelInfo;
import org.twilmes.sql.gremlin.schema.LabelUtil;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.util.FilterTranslator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.twilmes.sql.gremlin.schema.TableUtil.getTableDef;

/**
 * Builds a Gremlin traversal given a list of supported Rel nodes.
 * <p>
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class TraversalBuilder {

    public static GraphTraversal<?, ?> toTraversal(final List<RelNode> relList) {
        final GraphTraversal<?, ?> traversal = __.identity();
        TableDef tableDef = null;
        for (final RelNode rel : relList) {
            if (rel instanceof GremlinTableScan) {
                final GremlinTableScan tableScan = (GremlinTableScan) rel;
                tableDef = tableScan.getGremlinTable().getTableDef();
                traversal.hasLabel(tableDef.label);
            }
            if (rel instanceof GremlinFilter) {
                final GremlinFilter filter = (GremlinFilter) rel;
                final RexNode condition = filter.getCondition();
                final FilterTranslator translator = new FilterTranslator(tableDef, filter.getRowType().getFieldNames());
                final GraphTraversal<?, ?> predicates = translator.translateMatch(condition);
                for (final Step<?, ?> step : predicates.asAdmin().getSteps()) {
                    traversal.asAdmin().addStep(step);
                }
            }
        }
        return traversal;
    }

    public static void appendTraversal(final List<RelNode> relList, final GraphTraversal<?, ?> traversal) {
        TableDef tableDef = null;
        for (final RelNode rel : relList) {
            if (rel instanceof GremlinTableScan) {
                final GremlinTableScan tableScan = (GremlinTableScan) rel;
                tableDef = tableScan.getGremlinTable().getTableDef();
                traversal.hasLabel(tableDef.label);
            }
            if (rel instanceof GremlinFilter) {
                final GremlinFilter filter = (GremlinFilter) rel;
                final RexNode condition = filter.getCondition();
                final FilterTranslator translator = new FilterTranslator(tableDef, filter.getRowType().getFieldNames());
                final GraphTraversal<?, ?> predicates = translator.translateMatch(condition);
                for (final Step<?, ?> step : predicates.asAdmin().getSteps()) {
                    traversal.asAdmin().addStep(step);
                }
            }
        }
    }

    private static void findInsertionLocation(final Pair<String, String> pair, final List<Pair<String, String>> sorted)
            throws SQLException {
        final String first = sorted.get(0).getKey();
        final String last = sorted.get(sorted.size() - 1).getValue();
        if (pair.getValue().equals(last)) {
            sorted.add(swap(pair));
        } else if (pair.getKey().equals(last)) {
            sorted.add(pair);
        } else if (pair.getValue().equals(first)) {
            sorted.add(0, pair);
        } else if (pair.getKey().equals(first)) {
            sorted.add(0, swap(pair));
        } else {
            throw new SQLException("Failed to find insert location.");
        }
    }

    private static Pair<String, String> swap(final Pair<String, String> pair) {
        return new Pair<>(pair.right, pair.left);
    }

    @SneakyThrows
    public static GraphTraversal<?, ?> buildMatch(final GraphTraversalSource g,
                                                  final Map<String, GraphTraversal<?, ?>> tableIdTraversalMap,
                                                  final List<Pair<String, String>> joinPairs,
                                                  final SchemaConfig schemaConfig,
                                                  final Map<String, GremlinToEnumerableConverter> tableIdConverterMap) {
        final GraphTraversal<?, ?> startTraversal = g.V();
        final List<GraphTraversal<?, ?>> matchTraversals = new ArrayList<>();

        final List<Pair<String, String>> sorted = new ArrayList<>();
        if (joinPairs.size() > 1) {
            for (final Pair<String, String> pair : joinPairs) {
                if (sorted.size() == 0) {
                    sorted.add(pair);
                } else {
                    findInsertionLocation(pair, sorted);
                }
            }
        } else {
            // flip the pair if an edge is on the left...
            Pair<String, String> pair = joinPairs.get(0);
            if (!Objects.requireNonNull(getTableDef(tableIdConverterMap.get(pair.getKey()))).isVertex) {
                pair = new Pair<>(pair.getValue(), pair.getKey());
            }
            sorted.add(pair);
        }

        boolean first = true;
        // build match traversals
        for (final Pair<String, String> current : sorted) {
            final String leftId = current.getKey();
            final String rightId = current.getValue();
            final TableDef leftTableDef = getTableDef(tableIdConverterMap.get(leftId));
            final TableDef rightTableDef = getTableDef(tableIdConverterMap.get(rightId));

            // Null check
            if (rightTableDef == null || leftTableDef == null) {
                throw new SQLException("Left or right table definition is null.");
            }

            final GraphTraversal<?, ?> connectingTraversal =
                    getConnectingTraversal(leftTableDef.isVertex, rightTableDef.isVertex,
                            LabelUtil.getLabel(leftTableDef, rightTableDef, schemaConfig));

            addTraversal(connectingTraversal, tableIdTraversalMap.get(rightId).as(rightId));
            if (first) {
                addTraversal(startTraversal, tableIdTraversalMap.get(leftId));
                first = false;
            }
            matchTraversals.add(addTraversal(__.as(leftId), connectingTraversal));
        }

        return startTraversal.match(matchTraversals.toArray(new GraphTraversal<?, ?>[0]));
    }

    public static GraphTraversal<?, ?> getConnectingTraversal(final boolean leftIsVertex, final boolean rightIsVertex,
                                                              final LabelInfo labelInfo) {
        if (leftIsVertex && rightIsVertex) {
            return labelInfo.getDirection().equals(Direction.OUT) ? __.out(labelInfo.getLabel()) :
                    __.in(labelInfo.getLabel());
        } else if (leftIsVertex) {
            return labelInfo.getDirection().equals(Direction.OUT) ? __.outE(labelInfo.getLabel()) :
                    __.inE(labelInfo.getLabel());
        } else if (rightIsVertex) {
            return labelInfo.getDirection().equals(Direction.OUT) ? __.inV() : __.outV();
        }
        throw new ParseException("Illegal join of two edge tables.");
    }

    public static GraphTraversal<?, ?> addTraversal(final GraphTraversal<?, ?> traversal,
                                                    final GraphTraversal<?, ?> addMe) {
        for (final Step<?, ?> step : addMe.asAdmin().getSteps()) {
            traversal.asAdmin().addStep(step);
        }
        return traversal;
    }
}
