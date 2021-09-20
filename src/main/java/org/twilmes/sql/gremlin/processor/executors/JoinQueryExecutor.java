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

package org.twilmes.sql.gremlin.processor.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.visitors.JoinVisitor;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Executes queries that contain 1 or more joins.
 * <p>
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class JoinQueryExecutor extends QueryExecutor {
    private final SchemaConfig schemaConfig;
    private final JoinVisitor.JoinMetadata joinMetadata;

    public JoinQueryExecutor(final SchemaConfig schemaConfig, final SqlToGremlin.GremlinParseInfo gremlinParseInfo,
                             final JoinVisitor.JoinMetadata joinMetadata) {
        super(gremlinParseInfo, schemaConfig);
        this.schemaConfig = schemaConfig;
        this.joinMetadata = joinMetadata;
    }

    @Override
    public SqlGremlinQueryResult handle(final GraphTraversalSource g) throws SQLException {
        final GraphTraversal<?, ?> traversal = getJoinTraversal(joinMetadata, g);

        final List<String> fields =
                gremlinParseInfo.getGremlinSelectInfos().stream().map(SqlToGremlin.GremlinSelectInfo::getMappedName)
                        .collect(Collectors.toList());

        final SqlGremlinQueryResult sqlGremlinQueryResult = new SqlGremlinQueryResult(fields,
                ImmutableList.of(joinMetadata.getLeftTable(), joinMetadata.getRightTable()));

        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());

        executor.execute(new Pagination(new JoinDataReader(joinMetadata), traversal, sqlGremlinQueryResult));
        executor.shutdown();

        return sqlGremlinQueryResult;
    }

    private GraphTraversal<?, ?> getJoinTraversal(final JoinVisitor.JoinMetadata joinMetadata,
                                                  final GraphTraversalSource g) throws SQLException {
        // Hardcode vertex-vertex join for now.
        final String leftVertexLabel = joinMetadata.getLeftTable().label;
        final String rightVertexLabel = joinMetadata.getRightTable().label;
        final String edgeLabel = joinMetadata.getJoinColumn();

        final boolean leftInRightOut = schemaConfig.getRelationships().stream().anyMatch(
                r -> r.getInTable().toLowerCase().equals(leftVertexLabel.toLowerCase()) &&
                        r.getOutTable().toLowerCase().equals(rightVertexLabel.toLowerCase()));

        final boolean rightInLeftOut = schemaConfig.getRelationships().stream().anyMatch(
                r -> r.getInTable().toLowerCase().equals(rightVertexLabel.toLowerCase()) &&
                        r.getOutTable().toLowerCase().equals(leftVertexLabel.toLowerCase()));

        if (rightInLeftOut && leftInRightOut) {
            // TODO: Because we don't know if a bidirectional query will have enough results to reach out limit up front, we can
            // can't properly impose a limit. Right now this will return 2x the imposed LIMIT amount of queries.
            // TODO: Come back and fix this so that it handles bidirectional properly.
            return executeJoinTraversal(joinMetadata.getLeftTable(), joinMetadata.getRightTable(),
                    joinMetadata.getLeftRename(), joinMetadata.getRightRename(),
                    joinMetadata.getEdgeTable().label, g);
            // final GraphTraversal<?, ?> result2 = executeJoinTraversal(joinMetadata.getRightTable(), joinMetadata.getLeftTable(),
            //         joinMetadata.getEdgeTable().label, g);
        } else if (rightInLeftOut) {
            return executeJoinTraversal(joinMetadata.getRightTable(), joinMetadata.getLeftTable(),
                    joinMetadata.getRightRename(), joinMetadata.getLeftRename(),
                    joinMetadata.getEdgeTable().label, g);
        } else if (leftInRightOut) {
            return executeJoinTraversal(joinMetadata.getRightTable(), joinMetadata.getLeftTable(),
                    joinMetadata.getRightRename(), joinMetadata.getLeftRename(),
                    joinMetadata.getEdgeTable().label, g);
        } else {
            throw new SQLException("Failed to find in and out directionality of query.");
        }
    }

    private GraphTraversal<?, ?> executeJoinTraversal(final TableDef out, final TableDef in,
                                                      final String outRename, final String inRename,
                                                      final String edgeLabel, final GraphTraversalSource g)
            throws SQLException {
        final boolean inValues = gremlinParseInfo.getGremlinSelectInfos().stream()
                .anyMatch(s -> in.label.toLowerCase().equals(s.getTable().toLowerCase()));
        final boolean outValues = gremlinParseInfo.getGremlinSelectInfos().stream()
                .anyMatch(s -> out.label.toLowerCase().equals(s.getTable().toLowerCase()));
        final GraphTraversal<?, ?> traversal = g.V();
        if (inValues) {
            if (outValues) {
                // Doesn't really matter what way we traverse.
                final GraphTraversal<?, ?> graphTraversal1 = __.out(edgeLabel);
                appendSelectTraversal(out, graphTraversal1);
                traversal.hasLabel(out.label).in(edgeLabel).hasLabel(in.label)
                        .project(outRename, inRename)
                        .by(getSelectTraversal(in))
                        .by(graphTraversal1);
            } else {
                // Fastest way is to start at out and traverse to in.
                traversal.hasLabel(out.label).in(edgeLabel).hasLabel(in.label)
                        .project(inRename)
                        .by(getSelectTraversal(in));
            }
        } else if (outValues) {
            // Fastest way is to start at in and traverse to out.
            traversal.hasLabel(in.label).out(edgeLabel).hasLabel(out.label)
                    .project(outRename)
                    .by(getSelectTraversal(out));

        } else {
            throw new SQLException("Failed to find values to select in join query.");
        }

        imposeLimit(traversal);
        return traversal;
    }

    class JoinDataReader implements GetRowFromMap {
        final List<Pair<String, String>> tableColumnList = new ArrayList<>();

        JoinDataReader(final JoinVisitor.JoinMetadata joinMetadata) throws SQLException {
            for (final SqlToGremlin.GremlinSelectInfo gremlinSelectInfo : gremlinParseInfo.getGremlinSelectInfos()) {
                if (gremlinSelectInfo.getTable().equalsIgnoreCase(joinMetadata.getLeftRename())) {
                    tableColumnList.add(new Pair<>(joinMetadata.getLeftRename(),
                            TableUtil.getProperty(joinMetadata.getLeftTable(), gremlinSelectInfo.getColumn())));
                } else if (gremlinSelectInfo.getTable().equalsIgnoreCase(joinMetadata.getRightRename())) {
                    tableColumnList.add(new Pair<>(joinMetadata.getRightRename(),
                            TableUtil.getProperty(joinMetadata.getRightTable(), gremlinSelectInfo.getColumn())));
                } else {
                    throw new SQLException("Error, cannot marshal results, incorrect metadata.");
                }
            }
        }

        @Override
        public Object[] execute(final Map<String, Object> map) {
            final Object[] row = new Object[tableColumnList.size()];
            int i = 0;
            for (final Pair<String, String> tableColumn : tableColumnList) {
                final Optional<String> tableKey =
                        map.keySet().stream().filter(key -> key.equalsIgnoreCase(tableColumn.left)).findFirst();
                if (!tableKey.isPresent()) {
                    row[i++] = null;
                    continue;
                }

                final Optional<String> columnKey = ((Map<String, Object>) map.get(tableKey.get())).keySet().stream()
                        .filter(key -> key.equalsIgnoreCase(tableColumn.right)).findFirst();
                if (!columnKey.isPresent()) {
                    row[i++] = null;
                    continue;
                }
                row[i++] = ((Map<String, Object>) map.get(tableKey.get())).getOrDefault(columnKey.get(), null);
            }
            return row;
        }
    }
}
