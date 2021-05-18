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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public static GraphTraversal<?, ?> buildMatch(final GraphTraversalSource g,
                                                  final Map<String, GraphTraversal<?, ?>> tableIdTraversalMap,
                                                  final List<Pair<String, String>> joinPairs,
                                                  final SchemaConfig schemaConfig,
                                                  final Map<String, GremlinToEnumerableConverter> tableIdConverterMap) {
        final GraphTraversal<?, ?> startTraversal = g.V();
        final List<GraphTraversal<?, ?>> matchTraversals = new ArrayList<>();

        final ArrayList<Pair<String, String>> sorted = new ArrayList<>();
        if (joinPairs.size() > 1) {
            // sort join pairs
            Pair<String, String> startPair = joinPairs.get(0);
            final int pairs = joinPairs.size();
            while (sorted.size() < pairs - 1) {
                final String end = startPair.getValue();
                Optional<Pair<String, String>> next =
                        joinPairs.stream().filter(p -> p.getKey().equals(end)).findFirst();
                if (next.isPresent()) {
                    sorted.add(next.get());
                    startPair = next.get();
                    joinPairs.remove(startPair);
                } else {
                    next = joinPairs.stream().filter(p -> p.getValue().equals(end)).findFirst();
                    final Pair<String, String> val = next.get();
                    joinPairs.remove(val);
                    startPair = new Pair<>(val.getValue(), val.getKey());
                    sorted.add(startPair);
                }
            }
            // we'll have one left, insert it at the right spot
            Pair<String, String> last = joinPairs.get(0);
            boolean inserted = false;
            for (int i = 0; i < sorted.size(); i++) {
                if (last.getValue().equals(sorted.get(i).getKey())) {
                    sorted.add(i, last);
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                // reverse it
                last = new Pair<>(last.getValue(), last.getKey());
                for (int i = 0; i < sorted.size(); i++) {
                    if (last.getValue().equals(sorted.get(i).getKey())) {
                        sorted.add(i, last);
                        break;
                    }
                }
            }

            // now we have our pairs so build the match
            // find the first pair that starts with a vertex
            int startIndex = 0;
            for (; startIndex < sorted.size(); startIndex++) {
                final String startId = sorted.get(startIndex).getKey();
                if (getTableDef(tableIdConverterMap.get(startId)).isVertex) {
                    break;
                }
            }
        } else {
            // flip the pair if an edge is on the left...
            Pair<String, String> pair = joinPairs.get(0);
            if (!getTableDef(tableIdConverterMap.get(pair.getKey())).isVertex) {
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

            final LabelInfo labelInfo = LabelUtil.getLabel(leftTableDef, rightTableDef, schemaConfig);

            final GraphTraversal<?, ?> connectingTraversal;
            // TODO: This logic is much more complicated than it needs to be, simplify it.
            if (labelInfo.getDirection().equals(Direction.OUT)) {
                if (leftTableDef.isVertex && rightTableDef.isVertex) {
                    connectingTraversal = __.out(labelInfo.getLabel());
                } else if (leftTableDef.isVertex && !rightTableDef.isVertex) {
                    connectingTraversal = __.outE(labelInfo.getLabel());
                } else if (!leftTableDef.isVertex && rightTableDef.isVertex) {
                    connectingTraversal = __.inV();
                } else {
                    throw new ParseException("Illegal join of two edge tables.");
                }
            } else {
                if (leftTableDef.isVertex && rightTableDef.isVertex) {
                    connectingTraversal = __.in(labelInfo.getLabel());
                } else if (leftTableDef.isVertex && !rightTableDef.isVertex) {
                    connectingTraversal = __.inE(labelInfo.getLabel());
                } else if (!leftTableDef.isVertex && rightTableDef.isVertex) {
                    connectingTraversal = __.outV();
                } else {
                    throw new ParseException("Illegal join of two edge tables.");
                }
            }
            addTraversal(connectingTraversal, tableIdTraversalMap.get(rightId).as(rightId));

            if (first) {
                addTraversal(startTraversal, tableIdTraversalMap.get(leftId));
                first = false;
            }

            matchTraversals.add(addTraversal(__.as(leftId), connectingTraversal));
        }

        return startTraversal.match(matchTraversals.toArray(new GraphTraversal<?, ?>[0]));
    }

    public static GraphTraversal<?, ?> addTraversal(final GraphTraversal<?, ?> traversal,
                                                    final GraphTraversal<?, ?> addMe) {
        for (final Step<?, ?> step : addMe.asAdmin().getSteps()) {
            traversal.asAdmin().addStep(step);
        }
        return traversal;
    }
}
