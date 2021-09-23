package org.twilmes.sql.gremlin.adapter.nodes.operands;

import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlSelect;

public abstract class GremlinSqlOperand extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlOperand.class);
    SqlNode sqlNode;
    GremlinSqlMetadata gremlinSqlMetadata;

    GremlinSqlOperand(final SqlNode sqlNode, final GremlinSqlMetadata gremlinSqlMetadata) {
        super(sqlNode, gremlinSqlMetadata);
        this.sqlNode = sqlNode;
        this.gremlinSqlMetadata = gremlinSqlMetadata;
    }
}
