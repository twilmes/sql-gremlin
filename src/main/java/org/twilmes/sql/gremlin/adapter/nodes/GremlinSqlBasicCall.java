package org.twilmes.sql.gremlin.adapter.nodes;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.GremlinSqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlAsOperator;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.nodes.operator.aggregate.GremlinSqlAggFunction;
import java.sql.SQLException;
import java.util.List;

public class GremlinSqlBasicCall extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBasicCall.class);
    SqlBasicCall sqlBasicCall;
    GremlinSqlOperator gremlinSqlOperator;
    List<GremlinSqlNode> gremlinSqlNodes;

    public GremlinSqlBasicCall(final SqlBasicCall sqlBasicCall, final GremlinSqlMetadata gremlinSqlMetadata)
            throws SQLException {
        super(sqlBasicCall, gremlinSqlMetadata);
        this.sqlBasicCall = sqlBasicCall;
        gremlinSqlOperator = GremlinSqlFactory.createOperator(sqlBasicCall.getOperator());
        gremlinSqlNodes = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
    }

    void validate() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            if (gremlinSqlNodes.size() != 2) {
                throw new SQLException("Error, expected only two sub nodes for GremlinSqlBasicCall.");
            }
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() == 1) {
                throw new SQLException("Error, expected only one sub node for GremlinSqlAggFunction.");
            }
        }
    }

    private boolean shouldExecuteSqlAsTraversal() {
        return ((gremlinSqlOperator instanceof GremlinSqlAsOperator) &&
                (gremlinSqlNodes.size() == 2) && (gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier));
    }

    private boolean shouldExecuteSqlOperatorFunction() {
        return ((gremlinSqlOperator instanceof GremlinSqlAggFunction) &&
                (gremlinSqlNodes.size() == 1) && (gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier));
    }

    private boolean shouldExecuteBasicCall() {
        return ((gremlinSqlOperator instanceof GremlinSqlAsOperator) &&
                (gremlinSqlNodes.size() == 2) && (gremlinSqlNodes.get(1) instanceof GremlinSqlBasicCall));
    }

    public void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        validate();
        if (shouldExecuteSqlAsTraversal()) {
            appendSqlAsTraversal(graphTraversal);
        } else if (shouldExecuteSqlOperatorFunction()) {
            appendSqlOperatorTraversal(graphTraversal);
        } else if (shouldExecuteBasicCall()) {
            ((GremlinSqlBasicCall) gremlinSqlNodes.get(1)).appendTraversal(graphTraversal);
        } else {
            throw new SQLException("Error, unknown append type: in appendTraversal of SqlBasicCall.");
        }
    }

    private void appendSqlAsTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // Rename should already be applied, just need to add our traversal step.
        GremlinSqlTraversalEngine.applySqlIdentifier((GremlinSqlIdentifier)gremlinSqlNodes.get(0), gremlinSqlMetadata, graphTraversal);
    }

    private void appendSqlOperatorTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // Problem here is we will end up doing 'count().sqrt().value('foo') instead of value('foo').sqrt().count().
        // Need to walk down to the end and work our way backwards.


        // !!!! ---- Using function in agg function, we can keep calling our sub calls and then apply after.
        ((GremlinSqlOperator)gremlinSqlOperator).appendTraversal(graphTraversal);
    }

    public String getRename() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            if (gremlinSqlNodes.size() == 2 && gremlinSqlNodes.get(1) instanceof GremlinSqlIdentifier) {
                return ((GremlinSqlIdentifier) gremlinSqlNodes.get(1)).getName(1);
            }
            throw new SQLException("Expected 2 nodes with second node of SqlIdentifier for rename with AS.");
        }
        throw new SQLException("Unable to rename.");
    }
}
