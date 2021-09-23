package org.twilmes.sql.gremlin.adapter;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlBasicCall;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlSelect;
import org.twilmes.sql.gremlin.adapter.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlAsOperator;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlOperator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GremlinSqlFactory {
    private static GremlinSqlMetadata gremlinSqlMetadata = null;

    public static void setSqlMetadata(final GremlinSqlMetadata gremlinSqlMetadata1) {
        gremlinSqlMetadata = gremlinSqlMetadata1;
    }

    public static GremlinSqlMetadata getGremlinSqlMetadata() throws SQLException {
        if (gremlinSqlMetadata == null) {
            // TODO: Better error.
            throw new SQLException("Error: Schema must be set.");
        }
        return gremlinSqlMetadata;
    }

    public static GremlinSqlOperator createOperator(final SqlOperator sqlOperator) throws SQLException {
        if (sqlOperator instanceof SqlAsOperator) {
            return new GremlinSqlAsOperator((SqlAsOperator) sqlOperator, getGremlinSqlMetadata());
        }
        throw new SQLException("Error: Unknown operator: " + sqlOperator.getClass().getName());
    }

    public static GremlinSqlNode createNode(final SqlNode sqlNode) throws SQLException {
        if (sqlNode instanceof SqlSelect) {
            return new GremlinSqlSelect((SqlSelect) sqlNode, getGremlinSqlMetadata());
        } else if (sqlNode instanceof SqlBasicCall) {
            return new GremlinSqlBasicCall((SqlBasicCall) sqlNode, getGremlinSqlMetadata());
        } else if (sqlNode instanceof SqlIdentifier) {
            return new GremlinSqlIdentifier((SqlIdentifier) sqlNode, getGremlinSqlMetadata());
        }
        throw new SQLException("Error: Unknown node: " + sqlNode.getClass().getName());
    }

    public static List<GremlinSqlNode> createNodeList(final List<SqlNode> sqlNodes) throws SQLException {
        final List<GremlinSqlNode> gremlinSqlNodes = new ArrayList<>();
        for (final SqlNode sqlNode : sqlNodes) {
            gremlinSqlNodes.add(createNode(sqlNode));
        }
        return gremlinSqlNodes;
    }
}
