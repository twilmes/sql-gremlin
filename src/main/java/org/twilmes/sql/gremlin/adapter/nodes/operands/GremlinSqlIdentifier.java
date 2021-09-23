package org.twilmes.sql.gremlin.adapter.nodes.operands;

import org.apache.calcite.sql.SqlIdentifier;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlNode;
import java.sql.SQLException;

public class GremlinSqlIdentifier extends GremlinSqlNode {
    SqlIdentifier sqlIdentifier;

    public GremlinSqlIdentifier(final SqlIdentifier sqlIdentifier, final GremlinSqlMetadata gremlinSqlMetadata) {
        super(sqlIdentifier, gremlinSqlMetadata);
        this.sqlIdentifier = sqlIdentifier;
    }


    public String getName(final int idx) throws SQLException {
        if (idx >= sqlIdentifier.names.size()) {
            // TODO: FIX
            throw new SQLException("Index of identifier > size of name list for identifier");
        }
        return sqlIdentifier.names.get(idx);
    }

    public int getNameCount() {
        return sqlIdentifier.names.size();
    }
}
