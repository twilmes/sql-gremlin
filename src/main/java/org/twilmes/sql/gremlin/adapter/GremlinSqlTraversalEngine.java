package org.twilmes.sql.gremlin.adapter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.twilmes.sql.gremlin.adapter.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GremlinSqlTraversalEngine {

    public static void applySqlIdentifier(final GremlinSqlIdentifier sqlIdentifier,
                                          final GremlinSqlMetadata gremlinSqlMetadata,
                                          final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // Format of identifier is 'table'.'column => ['table', 'column']
        appendGraphTraversal(sqlIdentifier.getName(0), sqlIdentifier.getName(1), gremlinSqlMetadata, graphTraversal);
    }

    public static void appendGraphTraversal(final String table, final String column,
                                            final GremlinSqlMetadata gremlinSqlMetadata,
                                            final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        final TableDef tableDef = gremlinSqlMetadata.getTableDef(table);
        final SchemaConfig schemaConfig = gremlinSqlMetadata.getSchemaConfig();

        // Primary/foreign key, need to traverse appropriately.
        if (!column.toLowerCase().endsWith("_id")) {
            graphTraversal.by(
                    __.choose(__.has(column), __.values(column), __.constant("")));
        } else {
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
                                        __.inE().hasLabel(inVertex.get()).id().fold(),
                                        __.constant(new ArrayList<>())
                                ));
                    } else if (outVertex.isPresent()) {
                        graphTraversal.by(
                                __.coalesce(
                                        __.outE().hasLabel(outVertex.get()).fold(),
                                        __.constant(new ArrayList<>())
                                ));
                    } else {
                        graphTraversal.by(__.constant(new ArrayList<>()));
                    }
                }
            }
        }
    }

    public static GraphTraversal<?, ?> getTraversal(final String table, final List<String> columns,
                                                    final List<String> columnsRenamed,
                                                    final GremlinSqlMetadata gremlinSqlMetadata) throws SQLException {
        if (columns.isEmpty()) {
            throw new SQLException("Failed to find columns for table to use in traversal.");
        }
        final TableDef tableDef = gremlinSqlMetadata.getTableDef(table);
        final SchemaConfig schemaConfig = gremlinSqlMetadata.getSchemaConfig();

        final String firstColumn = columnsRenamed.remove(0);
        final String[] remaining = columnsRenamed.toArray(new String[] {});
        final GraphTraversal<?, ?> graphTraversal = __.project(firstColumn, remaining);

        for (final String column : columns) {
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
                                            __.inE().hasLabel(inVertex.get()).id().fold(),
                                            __.constant(new ArrayList<>())
                                    ));
                        } else if (outVertex.isPresent()) {
                            graphTraversal.by(
                                    __.coalesce(
                                            __.outE().hasLabel(outVertex.get()).fold(),
                                            __.constant(new ArrayList<>())
                                    ));
                        } else {
                            graphTraversal.by(__.constant(new ArrayList<>()));
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
}
