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

import lombok.AllArgsConstructor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public abstract class QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private static final int DEFAULT_PAGE_SIZE = 1000;
    protected final SqlToGremlin.GremlinParseInfo gremlinParseInfo;
    final SchemaConfig schemaConfig;
    private final int pageSize = DEFAULT_PAGE_SIZE;

    public abstract SqlGremlinQueryResult handle(GraphTraversalSource g) throws SQLException;

    // This function allows the limit to be overwritten.
    protected void imposeLimit(final GraphTraversal<?, ?> graphTraversal, final int limit) {
        if (limit != -1) {
            graphTraversal.limit(limit);
        }
    }

    protected void imposeLimit(final GraphTraversal<?, ?> graphTraversal) {
        imposeLimit(graphTraversal, gremlinParseInfo.getLimit());
    }

    protected void appendSelectTraversal(final TableDef tableDef, final GraphTraversal<?, ?> graphTraversal)
            throws SQLException {
        final List<String> columns = new ArrayList<>();
        for (final SqlToGremlin.GremlinSelectInfo selectInfo : gremlinParseInfo.getGremlinSelectInfos()) {
            if (selectInfo.getTable().toLowerCase().equals(tableDef.label.toLowerCase())) {
                columns.add(TableUtil.getProperty(tableDef, selectInfo.getColumn()));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("Failed to find columns for table to use in traversal.");
        }

        final String firstColumn = columns.remove(0);
        final String[] remaining = columns.toArray(new String[] {});
        graphTraversal.project(firstColumn, remaining);
        final List<String> columns2 = new ArrayList<>();
        columns2.add(firstColumn);
        columns2.addAll(Arrays.asList(remaining));
        for (final String column : columns2) {
            // Primary/foreign key, need to traverse appropriately.
            if (column.toLowerCase().endsWith("_id")) {
                // It's this vertex
                if (column.toLowerCase().startsWith(tableDef.label)) {
                    graphTraversal.by(__.id());
                } else {
                    if (tableDef.isVertex) {
                        final List<TableRelationship> vertices = schemaConfig.getRelationships().stream()
                                .filter(r -> column.toLowerCase().startsWith(r.getEdgeLabel().toLowerCase()))
                                .collect(Collectors.toList());

                        final Optional<String> inVertex = vertices.stream()
                                .filter(r -> r.getOutTable().equalsIgnoreCase(tableDef.label))
                                .map(TableRelationship::getEdgeLabel).findFirst();
                        final Optional<String> outVertex = vertices.stream()
                                .filter(r -> r.getOutTable().equalsIgnoreCase(tableDef.label))
                                .map(TableRelationship::getEdgeLabel).findFirst();
                        if (inVertex.isPresent()) {
                            graphTraversal.by(__.inE().hasLabel(inVertex.get()).id());
                        } else if (outVertex.isPresent()) {
                            graphTraversal.by(__.outE().hasLabel(outVertex.get()).id());
                        } else {
                            graphTraversal.by(__.constant(""));
                        }
                    }
                }
            } else {
                graphTraversal.by(__.values(column));
            }
        }
    }

    protected GraphTraversal<?, ?> getSelectTraversal(final TableDef tableDef) throws SQLException {
        final List<String> columns = new ArrayList<>();
        for (final SqlToGremlin.GremlinSelectInfo selectInfo : gremlinParseInfo.getGremlinSelectInfos()) {
            if (selectInfo.getTable().toLowerCase().equals(tableDef.label.toLowerCase())) {
                columns.add(TableUtil.getProperty(tableDef, selectInfo.getColumn()));
            }
        }

        if (columns.isEmpty()) {
            throw new SQLException("Failed to find columns for table to use in traversal.");
        }

        final String firstColumn = columns.remove(0);
        final String[] remaining = columns.toArray(new String[] {});
        final GraphTraversal<?, ?> graphTraversal = __.project(firstColumn, remaining);
        final List<String> columns2 = new ArrayList<>();
        columns2.add(firstColumn);
        columns2.addAll(Arrays.asList(remaining));
        for (final String column : columns2) {
            // Primary/foreign key, need to traverse appropriately.
            if (column.toLowerCase().endsWith("_id")) {
                // It's this vertex
                if (column.toLowerCase().startsWith(tableDef.label)) {
                    graphTraversal.by(__.id());
                } else {
                    if (tableDef.isVertex) {
                        final List<TableRelationship> edges = schemaConfig.getRelationships().stream()
                                .filter(r -> column.toLowerCase().startsWith(r.getEdgeLabel().toLowerCase()))
                                .collect(Collectors.toList());

                        final Optional<String> inVertex = edges.stream()
                                .filter(r -> r.getOutTable().equalsIgnoreCase(tableDef.label))
                                .map(TableRelationship::getEdgeLabel).findFirst();
                        final Optional<String> outVertex = edges.stream()
                                .filter(r -> r.getInTable().equalsIgnoreCase(tableDef.label))
                                .map(TableRelationship::getEdgeLabel).findFirst();
                        if (inVertex.isPresent()) {
                            graphTraversal.by(
                                    __.coalesce(
                                            __.inE().hasLabel(inVertex.get()).id(),
                                            __.constant("")
                                    ));
                        } else if (outVertex.isPresent()) {
                            graphTraversal.by(
                                    __.coalesce(
                                            __.outE().hasLabel(outVertex.get()).id(),
                                            __.constant("")
                                    ));
                        } else {
                            graphTraversal.by(__.constant(""));
                        }
                    }
                }
            } else {
                graphTraversal.by(
                        __.choose(__.has(column), __.values(column), __.constant("")));
            }
        }
        return graphTraversal;
    }

    /**
     * converts input row results and insert them into sqlGremlinQueryResult
     */
    void convertAndInsertResult(final SqlGremlinQueryResult sqlGremlinQueryResult, final List<Object> rows) {
        final List<List<Object>> finalRowResult = new ArrayList<>();
        for (final Object row : rows) {
            final List<Object> convertedRow = new ArrayList<>();
            if (row instanceof Object[]) {
                convertedRow.addAll(Arrays.asList((Object[]) row));
            } else {
                convertedRow.add(row);
            }
            finalRowResult.add(convertedRow);
        }
        sqlGremlinQueryResult.addResults(finalRowResult);
    }

    interface GetRowFromMap {
        Object[] execute(Map<String, Object> input);
    }

    @AllArgsConstructor
    public class Pagination implements Runnable {
        private final GetRowFromMap getRowFromMap;
        private final GraphTraversal<?, ?> traversal;
        SqlGremlinQueryResult sqlGremlinQueryResult;

        @Override
        public void run() {
            try {
                while (traversal.hasNext()) {
                    final List<Object> rows = new ArrayList<>();
                    traversal.next(pageSize)
                            .forEach(map -> rows.add(getRowFromMap.execute((Map<String, Object>) map)));
                    convertAndInsertResult(sqlGremlinQueryResult, rows);
                }
                // If we run out of traversal data (or hit our limit), stop and signal to the result that it is done.
                sqlGremlinQueryResult.assertIsEmpty();
            } catch (final Exception e) {
                LOGGER.error("Encountered exception", e);
                sqlGremlinQueryResult.assertIsEmpty();
            }
        }
    }
}
