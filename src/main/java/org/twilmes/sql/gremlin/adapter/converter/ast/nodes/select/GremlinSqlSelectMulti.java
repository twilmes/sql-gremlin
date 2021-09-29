/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlAsOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.join.GremlinSqlJoinComparison;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import org.twilmes.sql.gremlin.adapter.results.pagination.JoinDataReader;
import org.twilmes.sql.gremlin.adapter.results.pagination.Pagination;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlSelect for a JOIN operation.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlSelectMulti extends GremlinSqlSelect {
    // Multi is a JOIN.
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelectMulti.class);
    private final SqlSelect sqlSelect;
    private final SqlMetadata sqlMetadata;
    private final GraphTraversalSource g;
    private final SqlJoin sqlJoin;

    public GremlinSqlSelectMulti(final SqlSelect sqlSelect, final SqlJoin sqlJoin,
                                 final SqlMetadata sqlMetadata, final GraphTraversalSource g) {
        super(sqlSelect, sqlMetadata);
        this.sqlMetadata = sqlMetadata;
        this.sqlSelect = sqlSelect;
        this.g = g;
        this.sqlJoin = sqlJoin;
    }

    @Override
    protected void runTraversalExecutor(final GraphTraversal<?, ?> graphTraversal,
                                        final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());
        final Map<String, List<String>> tableColumns = sqlMetadata.getColumnOutputListMap();
        if (tableColumns.keySet().size() != 2) {
            throw new SQLException("Error: Join expects two tables only.");
        }
        executor.execute(new Pagination(new JoinDataReader(tableColumns), graphTraversal, sqlGremlinQueryResult));
        executor.shutdown();

    }

    @Override
    public GraphTraversal<?, ?> generateTraversal() throws SQLException {
        final JoinType joinType = sqlJoin.getJoinType();
        final JoinConditionType conditionType = sqlJoin.getConditionType();

        final GremlinSqlBasicCall left =
                GremlinSqlFactory.createNodeCheckType(sqlJoin.getLeft(), GremlinSqlBasicCall.class);
        final GremlinSqlBasicCall right =
                GremlinSqlFactory.createNodeCheckType(sqlJoin.getRight(), GremlinSqlBasicCall.class);
        final GremlinSqlJoinComparison gremlinSqlJoinComparison =
                GremlinSqlFactory.createJoinEquality(sqlJoin.getCondition());

        if (!joinType.name().equals(JoinType.INNER.name())) {
            throw new SQLException("Error, only INNER joins are supported.");
        }
        if (!conditionType.equals(JoinConditionType.ON)) {
            throw new SQLException("Error, only joins with ON conditions are supported.");
        }
        if ((left.getGremlinSqlNodes().size() != 2) || (right.getGremlinSqlNodes().size() != 2)) {
            throw new SQLException("Error: Expected 2 operands for left, right, and condition.");
        }
        if (!(left.getGremlinSqlOperator() instanceof GremlinSqlAsOperator) ||
                !(right.getGremlinSqlOperator() instanceof GremlinSqlAsOperator)) {
            throw new SQLException("Error: Expected left and right to have AS operators.");
        }
        final GremlinSqlAsOperator leftAsOperator = (GremlinSqlAsOperator) left.getGremlinSqlOperator();
        final String leftTableName = sqlMetadata.getActualTableName(leftAsOperator.getName(0, 1));
        final String leftTableRename = leftAsOperator.getName(1, 0);
        sqlMetadata.addRenamedTable(leftTableName, leftTableRename);
        final String leftColumn = gremlinSqlJoinComparison.getColumn(leftTableRename);

        final GremlinSqlAsOperator rightAsOperator = (GremlinSqlAsOperator) right.getGremlinSqlOperator();
        final String rightTableName = sqlMetadata.getActualTableName(rightAsOperator.getName(0, 1));
        final String rightTableRename = rightAsOperator.getName(1, 0);
        sqlMetadata.addRenamedTable(rightTableName, rightTableRename);
        final String rightColumn = gremlinSqlJoinComparison.getColumn(rightTableRename);

        if (!sqlMetadata.getIsColumnEdge(leftTableRename, leftColumn) ||
                !sqlMetadata.getIsColumnEdge(rightTableRename, rightColumn)) {
            throw new SQLException("Error: Joins can only be performed on vertices that are connected by an edge.");
        }

        if (!rightColumn.equals(leftColumn)) {
            throw new SQLException(
                    "Error: Joins can only be performed on vertices that are connected by a mutual edge.");
        }

        final String edgeLabel = sqlMetadata.getColumnEdgeLabel(leftColumn);
        // Cases to consider:
        //  1. rightLabel == leftLabel
        //  2. rightLabel != leftLabel, rightLabel->leftLabel
        //  3. rightLabel != leftLabel, leftLabel->rightLabel
        //  4. rightLabel != leftLabel, rightLabel->leftLabel, leftLabel->rightLabel
        // Case 1 & 4 are logically equivalent.

        // Determine which is in and which is out.
        final boolean leftInRightOut = sqlMetadata.isLeftInRightOut(leftTableName, rightTableName);
        final boolean rightInLeftOut = sqlMetadata.isRightInLeftOut(leftTableName, rightTableName);

        final String inVLabel;
        final String outVLabel;
        final String inVRename;
        final String outVRename;
        if (leftInRightOut && rightInLeftOut && (leftTableName.equals(rightTableName))) {
            // Vertices of same label connected by an edge.
            // Doesn't matter how we assign these, but renames need to be different.
            inVLabel = leftTableName;
            outVLabel = leftTableName;
            inVRename = leftTableRename;
            outVRename = rightTableRename;
        } else if (leftInRightOut) {
            // Left vertex is in, right vertex is out
            inVLabel = leftTableName;
            outVLabel = rightTableName;
            inVRename = leftTableRename;
            outVRename = rightTableRename;
        } else if (rightInLeftOut) {
            // Right vertex is in, left vertex is out
            inVLabel = rightTableName;
            outVLabel = leftTableName;
            inVRename = rightTableRename;
            outVRename = leftTableRename;
        } else {
            inVLabel = "";
            outVLabel = "";
            inVRename = "";
            outVRename = "";
        }

        final List<GremlinSqlNode> gremlinSqlNodesIn = new ArrayList<>();
        final List<GremlinSqlNode> gremlinSqlNodesOut = new ArrayList<>();
        for (final SqlNode sqlNode : sqlSelect.getSelectList().getList()) {
            if (GremlinSqlFactory.isTable(sqlNode, inVRename)) {
                gremlinSqlNodesIn.add(GremlinSqlFactory.createNode(sqlNode));
            } else if (GremlinSqlFactory.isTable(sqlNode, outVRename)) {
                gremlinSqlNodesOut.add(GremlinSqlFactory.createNode(sqlNode));
            }
        }

        final GraphTraversal<?, ?> graphTraversal = g.E().hasLabel(edgeLabel)
                .where(__.inV().hasLabel(inVLabel))
                .where(__.outV().hasLabel(outVLabel));
        applyGroupBy(graphTraversal, inVRename, outVRename);
        graphTraversal.project(inVRename, outVRename);
        applyColumnRetrieval(graphTraversal, inVRename, gremlinSqlNodesIn, StepDirection.In);
        applyColumnRetrieval(graphTraversal, outVRename, gremlinSqlNodesOut, StepDirection.Out);
        return graphTraversal;
    }

    // TODO: Fill in group by and place in correct position of traversal.
    protected void applyGroupBy(final GraphTraversal<?, ?> graphTraversal, final String inVRename, final String outVRename) throws SQLException {
        if ((sqlSelect.getGroup() != null) && (sqlSelect.getGroup().getList().size() > 0)) {
            final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
            for (final SqlNode sqlNode : sqlSelect.getGroup().getList()) {
                gremlinSqlIdentifiers.add(GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class));
            }

            graphTraversal.order();
            for (final GremlinSqlIdentifier gremlinSqlIdentifier : gremlinSqlIdentifiers) {
                final String table = sqlMetadata.getActualTableName(gremlinSqlIdentifier.getName(0));
                final String column = sqlMetadata.getActualColumnName(sqlMetadata.getTableDef(table), gremlinSqlIdentifier.getName(1));
                if (inVRename.equals(table)) {
                    graphTraversal.by(__.inV().hasLabel(table).values(sqlMetadata.getActualColumnName(sqlMetadata.getTableDef(table), column)));
                } else if (outVRename.equals(table)) {
                    graphTraversal.by(__.outV().hasLabel(table).values(sqlMetadata.getActualColumnName(sqlMetadata.getTableDef(table), column)));
                } else {
                    throw new SQLException(String.format("Error, unable to group table %s.", table));
                }
            }
        }
        /**
         .order()
         .by(outV().hasLabel('airport').values('country'))
         .by(inV().hasLabel('airport').values('country'))
         */
    }
}