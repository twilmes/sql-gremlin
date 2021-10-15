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

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.logic;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlTraversalAppender;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GremlinSqlBinaryOperator extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBinaryOperator.class);
    private static final Map<SqlKind, GremlinSqlTraversalAppender> BINARY_APPENDERS =
            new HashMap<SqlKind, GremlinSqlTraversalAppender>() {
                {
                    put(SqlKind.EQUALS, null);
                    put(SqlKind.NOT_EQUALS, null);
                    put(SqlKind.GREATER_THAN, null);
                    put(SqlKind.GREATER_THAN_OR_EQUAL, null);
                    put(SqlKind.LESS_THAN, null);
                    put(SqlKind.LESS_THAN_OR_EQUAL, null);
                }
            };
    private final SqlBinaryOperator sqlBinaryOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlBinaryOperator(final SqlBinaryOperator sqlBinaryOperator,
                                    final List<GremlinSqlNode> sqlOperands,
                                    final SqlMetadata sqlMetadata) {
        super(sqlBinaryOperator, sqlOperands, sqlMetadata);
        this.sqlBinaryOperator = sqlBinaryOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = sqlOperands;
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {

    }
}