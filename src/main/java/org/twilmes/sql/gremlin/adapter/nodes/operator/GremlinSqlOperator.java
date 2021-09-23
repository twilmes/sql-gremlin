package org.twilmes.sql.gremlin.adapter.nodes.operator;

import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlSelect;
import org.twilmes.sql.gremlin.rel.GremlinTraversalRel;
import java.sql.SQLException;

@AllArgsConstructor
public abstract class GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlOperator.class);
    SqlOperator sqlOperator;
    GremlinSqlMetadata gremlinSqlMetadata;

    public abstract void appendTraversal(GraphTraversal<?, ?> graphTraversal) throws SQLException;
}
