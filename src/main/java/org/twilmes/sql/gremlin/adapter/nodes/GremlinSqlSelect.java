package org.twilmes.sql.gremlin.adapter.nodes;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.GremlinSqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlAsOperator;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlOperator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GremlinSqlSelect extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelect.class);
    private final GraphTraversalSource g;
    private final SqlSelect sqlSelect;

    public GremlinSqlSelect(final SqlSelect sqlSelect, final GremlinSqlMetadata gremlinSqlMetadata) {
        super(sqlSelect, gremlinSqlMetadata);
        this.sqlSelect = sqlSelect;
        this.g = gremlinSqlMetadata.getG();
    }

    public GraphTraversal<?, ?> executeTraversal() throws SQLException {
        final GraphTraversal<?, ?> traversal;

        if (sqlSelect.getFrom() == null) {
            throw new SQLException("Error: GremlinSqlSelect expects from component.");
        }

        if (sqlSelect.getSelectList() == null) {
            throw new SQLException("Error: GremlinSqlSelect expects select list component.");
        }
        traversal = getFromTraversal();
        applySelectList(traversal);
        return traversal;
        /*
        if (sqlSelect.getHaving() != null) {
            applyGroupBy();
        }

        if (sqlSelect.getGroup() != null) {
            applyGroupBy();
        }
         */

    }

    private GraphTraversal<?, ?> getFromTraversal() throws SQLException {
        final SqlNode sqlNode = sqlSelect.getFrom();
        if (!(sqlNode instanceof SqlBasicCall)) {
            throw new SQLException("Unexpected format for FROM.");
        }
        final SqlBasicCall sqlBasicCall = (SqlBasicCall) (sqlNode);

        // TODO: Maybe AsOperator should hold operands??
        final GremlinSqlOperator gremlinSqlOperator = GremlinSqlFactory.createOperator(sqlBasicCall.getOperator());
        if (!(gremlinSqlOperator instanceof GremlinSqlAsOperator)) {
            throw new SQLException("Unexpected format for FROM.");
        }
        final List<GremlinSqlNode> gremlinSqlOperands = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
        final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
        for (final GremlinSqlNode gremlinSqlOperand : gremlinSqlOperands) {
            if (!(gremlinSqlOperand instanceof GremlinSqlIdentifier)) {
                throw new SQLException("Unexpected format for FROM.");
            }
            gremlinSqlIdentifiers.add((GremlinSqlIdentifier) gremlinSqlOperand);
        }

        if (gremlinSqlIdentifiers.size() != 2) {
            throw new SQLException("Expected gremlin sql identifiers list size to be 2.");
        }

        // Currently: g.V().hasLabel("PERSON").project("Person").by(__.choose(__.has("NAME"),__.values("NAME"),__.constant("")))
        // Should be: g.V().hasLabel("PERSON").project("Person").by(
        //                                      .project("name")
        //                                          .by(__.choose(__.has("NAME"),__.values("NAME"),__.constant(""))
        //                                      )
        final String label = gremlinSqlIdentifiers.get(0).getName(1);
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);
        final GraphTraversal<?, ?> traversal = gremlinSqlMetadata.isVertex(label) ? g.V() : g.E();
        traversal.hasLabel(label).project(projectLabel);



        // TODO: Need to apply.
        final List<String> columnRenames = new ArrayList<>();
        for (final GremlinSqlNode gremlinSqlNode : sqlNodeList) {
            if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
                columnRenames.add(((GremlinSqlIdentifier) gremlinSqlNode).getName(0));
            } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
                columnRenames.add(((GremlinSqlBasicCall) gremlinSqlNode).getRename());
            } else {
                throw new SQLException(
                        "Error: Unknown sql node type for select list " + gremlinSqlNode.getClass().getName());
            }
        }

        return traversal;
    }

    private void applyFetch() {
        // ??
    }

    private void applyGroupBy() {
        // ??
    }

    void applySelectList(final GraphTraversal<?, ?> traversal) throws SQLException {


        final List<GremlinSqlNode> sqlNodeList = GremlinSqlFactory.createNodeList(sqlSelect.getSelectList().getList());
        for (final GremlinSqlNode gremlinSqlNode : sqlNodeList) {
            if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
                GremlinSqlTraversalEngine
                        .applySqlIdentifier((GremlinSqlIdentifier) gremlinSqlNode, gremlinSqlMetadata, traversal);
            } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
                ((GremlinSqlBasicCall) gremlinSqlNode).appendTraversal(traversal);
            } else {
                throw new SQLException(
                        "Error: Unknown sql node type for select list " + gremlinSqlNode.getClass().getName());
            }
        }
    }
}
