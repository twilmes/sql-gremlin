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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.SqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlAsOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import org.twilmes.sql.gremlin.adapter.results.pagination.Pagination;
import org.twilmes.sql.gremlin.adapter.results.pagination.SimpleDataReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlSelect for a non-JOIN operation.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlSelectSingle extends GremlinSqlSelect {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelectSingle.class);
    private final SqlSelect sqlSelect;
    private final SqlMetadata sqlMetadata;
    private final GraphTraversalSource g;
    private final SqlBasicCall sqlBasicCall;

    public GremlinSqlSelectSingle(final SqlSelect sqlSelect,
                                  final SqlBasicCall sqlBasicCall,
                                  final SqlMetadata sqlMetadata, final GraphTraversalSource g) {
        super(sqlSelect, sqlMetadata);
        this.sqlSelect = sqlSelect;
        this.sqlMetadata = sqlMetadata;
        this.g = g;
        this.sqlBasicCall = sqlBasicCall;
    }

    @Override
    protected void runTraversalExecutor(final GraphTraversal<?, ?> graphTraversal,
                                        final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());
        final List<List<String>> columns = new ArrayList<>(sqlMetadata.getColumnOutputListMap().values());
        if (columns.size() != 1) {
            throw new SQLException("Error: Single select has multi-table return.");
        }
        executor.execute(
                new Pagination(new SimpleDataReader(sqlMetadata.getTables().iterator().next(), columns.get(0)),
                        graphTraversal, sqlGremlinQueryResult));
        executor.shutdown();
    }

    @Override
    public GraphTraversal<?, ?> generateTraversal() throws SQLException {
        if (sqlSelect.getSelectList() == null) {
            throw new SQLException("Error: GremlinSqlSelect expects select list component.");
        }

        final GremlinSqlOperator gremlinSqlOperator =
                GremlinSqlFactory.createOperator(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList());
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

        final GraphTraversal<?, ?> graphTraversal =
                SqlTraversalEngine.generateInitialSql(gremlinSqlIdentifiers, sqlMetadata, g);
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);
        applyGroupBy(graphTraversal);
        applyColumnRetrieval(graphTraversal, projectLabel, GremlinSqlFactory.createNodeList(sqlSelect.getSelectList().getList()));

        if (sqlMetadata.getRenamedColumns() == null) {
            throw new SQLException("Error: Column rename list is empty.");
        }
        if (sqlMetadata.getTables().size() != 1) {
            throw new SQLException("Error: Expected one table for traversal execution.");
        }
        return graphTraversal;
    }

    public String getStringTraversal() throws SQLException {
        return GroovyTranslator.of("g").translate(generateTraversal().asAdmin().getBytecode());
    }

    // TODO: Fill in group by and place in correct position of traversal.
    protected void applyGroupBy(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlSelect.getGroup() != null) {
            final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
            for (final SqlNode sqlNode : sqlSelect.getGroup().getList()) {
                gremlinSqlIdentifiers.add(GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class));
            }
            for (final GremlinSqlIdentifier gremlinSqlIdentifier : gremlinSqlIdentifiers) {
                final String table = sqlMetadata.getActualTableName(gremlinSqlIdentifier.getName(0));
                final String column = sqlMetadata.getActualColumnName(sqlMetadata.getTableDef(table), gremlinSqlIdentifier.getName(0));
                graphTraversal.by(__.values(sqlMetadata.getActualColumnName(sqlMetadata.getTableDef(table), column)));
            }
        }
    }
}