package org.twilmes.sql.gremlin.adapter.nodes;

import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;

@AllArgsConstructor
public abstract class GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlNode.class);
    SqlNode sqlNode;
    GremlinSqlMetadata gremlinSqlMetadata;
}
