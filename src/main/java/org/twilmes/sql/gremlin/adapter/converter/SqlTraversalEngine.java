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

package org.twilmes.sql.gremlin.adapter.converter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.StepDirection;
import org.twilmes.sql.gremlin.adapter.converter.schema.SchemaConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableDef;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableRelationship;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Traversal engine for SQL-Gremlin. This module is responsible for generating the gremlin traversals.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class SqlTraversalEngine {

    public static GraphTraversal<?, ?> generateInitialSql(final List<GremlinSqlIdentifier> gremlinSqlIdentifiers,
                                                          final SqlMetadata sqlMetadata,
                                                          final GraphTraversalSource g) throws SQLException {
        if (gremlinSqlIdentifiers.size() != 2) {
            throw new SQLException("Expected gremlin sql identifiers list size to be 2.");
        }

        // Should move all this into the traversal engine.
        final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);

        final GraphTraversal<?, ?> graphTraversal = sqlMetadata.isVertex(label) ? g.V() : g.E();
        graphTraversal.hasLabel(label).project(projectLabel);

        sqlMetadata.addRenamedTable(label, projectLabel);

        return graphTraversal;
    }

    public static GraphTraversal<?, ?> getEmptyTraversal(final StepDirection direction) {
        switch (direction) {
            case Out:
                return __.outV();
            case In:
                return __.inV();
        }
        return __.__();
    }

    public static GraphTraversal<?, ?> getEmptyTraversal() {
        return getEmptyTraversal(StepDirection.None);
    }


    public static void applyTraversal(final GraphTraversal<?, ?> graphTraversal,
                                      final GraphTraversal<?, ?> subGraphTraversal) {
        graphTraversal.by(subGraphTraversal);
    }

    public static void applySqlIdentifier(final GremlinSqlIdentifier sqlIdentifier,
                                          final SqlMetadata sqlMetadata,
                                          final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // Format of identifier is 'table'.'column => ['table', 'column']
        appendGraphTraversal(sqlIdentifier.getName(0), sqlIdentifier.getName(1), sqlMetadata, graphTraversal);
    }

    public static GraphTraversal<?, ?> applyColumnRenames(final List<String> columnsRenamed) throws SQLException {
        final String firstColumn = columnsRenamed.remove(0);
        final String[] remaining = columnsRenamed.toArray(new String[] {});

        return __.project(firstColumn, remaining);
    }

    private static void appendGraphTraversal(final String table, final String column,
                                             final SqlMetadata sqlMetadata,
                                             final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        final TableDef tableDef = sqlMetadata.getTableDef(table);
        final SchemaConfig schemaConfig = sqlMetadata.getSchemaConfig();
        final String columnName = sqlMetadata.getActualColumnName(tableDef, column);

        // Primary/foreign key, need to traverse appropriately.
        if (!columnName.toLowerCase().endsWith("_id")) {
            graphTraversal.choose(__.has(columnName), __.values(columnName), __.constant(""));
        } else {
            // It's this vertex
            if (columnName.toLowerCase().startsWith(tableDef.label)) {
                graphTraversal.id();
            } else {
                if (tableDef.isVertex) {
                    final List<TableRelationship> edges = schemaConfig.getRelationships().stream()
                            .filter(r -> columnName.toLowerCase().startsWith(r.getEdgeLabel().toLowerCase()))
                            .collect(Collectors.toList());

                    final Optional<String> inVertex = edges.stream()
                            .filter(r -> r.getOutTable().equalsIgnoreCase(tableDef.label))
                            .map(TableRelationship::getEdgeLabel).findFirst();
                    final Optional<String> outVertex = edges.stream()
                            .filter(r -> r.getInTable().equalsIgnoreCase(tableDef.label))
                            .map(TableRelationship::getEdgeLabel).findFirst();
                    if (inVertex.isPresent()) {
                        graphTraversal.coalesce(
                                __.inE().hasLabel(inVertex.get()).id().fold(),
                                __.constant(new ArrayList<>())
                        );
                    } else if (outVertex.isPresent()) {
                        graphTraversal.coalesce(
                                __.outE().hasLabel(outVertex.get()).fold(),
                                __.constant(new ArrayList<>())
                        );
                    } else {
                        graphTraversal.constant(new ArrayList<>());
                    }
                }
            }
        }
    }
}