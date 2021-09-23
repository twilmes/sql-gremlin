package org.twilmes.sql.gremlin.adapter.nodes.operator;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlSelect;
import org.twilmes.sql.gremlin.adapter.nodes.operands.GremlinSqlIdentifier;
import java.sql.SQLException;

public class GremlinSqlAsOperator extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlAsOperator.class);
    SqlAsOperator sqlAsOperator;
    GremlinSqlMetadata gremlinSqlMetadata;

    public GremlinSqlAsOperator(final SqlAsOperator sqlAsOperator, GremlinSqlMetadata gremlinSqlMetadata) {
        super(sqlAsOperator, gremlinSqlMetadata);
        this.sqlAsOperator = sqlAsOperator;
        this.gremlinSqlMetadata = gremlinSqlMetadata;
    }

    public String getRename(GremlinSqlIdentifier gremlinSqlIdentifier) throws SQLException {
        return gremlinSqlIdentifier.getName(1);
    }

    @Override
    public void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // As does not do any appending.
    }
}
