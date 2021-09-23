package org.twilmes.sql.gremlin.adapter;

import lombok.Getter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.schema.GremlinSchema;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import java.sql.SQLException;

@Getter
public class GremlinSqlMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlMetadata.class);
    private final GraphTraversalSource g;
    private final SchemaConfig schemaConfig;
    private final GremlinSchema gremlinSchema;

    public GremlinSqlMetadata(final GraphTraversalSource g, final SchemaConfig schemaConfig) {
        this.g = g;
        this.schemaConfig = schemaConfig;
        gremlinSchema = new GremlinSchema(schemaConfig);
    }

    public boolean isVertex(final String table) throws SQLException {
        LOGGER.info("isVertex({})", table);
        for (final TableConfig tableConfig : schemaConfig.getTables()) {
            LOGGER.info("{}.equalsIgnoreCase({}) => {}", tableConfig.getName(), table,
                    tableConfig.getName().equalsIgnoreCase(table));
            if (tableConfig.getName().equalsIgnoreCase(table)) {
                return true;
            }
        }
        for (final TableRelationship tableRelationship : schemaConfig.getRelationships()) {
            if (tableRelationship.getEdgeLabel().equalsIgnoreCase(table)) {
                return false;
            }
        }
        // TODO: Fix error.
        throw new SQLException("Error: Table {} does not exist.", table);
    }


    public TableDef getTableDef(final String table) throws SQLException {
        for (final TableConfig tableConfig : schemaConfig.getTables()) {
            if (tableConfig.getName().equalsIgnoreCase(table)) {
                return GremlinSchema.getTableDef(tableConfig, schemaConfig);
            }
        }
        for (final TableRelationship tableRelationship : schemaConfig.getRelationships()) {
            if (tableRelationship.getEdgeLabel().equalsIgnoreCase(table)) {
                return GremlinSchema.getTableDef(tableRelationship);
            }
        }
        // TODO: Fix error.
        throw new SQLException("Error: Table {} does not exist.", table);
    }
}
