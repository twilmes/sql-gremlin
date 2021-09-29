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

package org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.aggregate;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.operator.GremlinSqlTraversalAppender;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlAggFunction.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class GremlinSqlAggFunction extends GremlinSqlOperator {
    // See SqlKind.AGGREGATE for list of aggregate functions in Calcite.
    private static final Map<SqlKind, GremlinSqlTraversalAppender> AGGREGATE_APPENDERS =
            new HashMap<SqlKind, GremlinSqlTraversalAppender>() {{
                put(SqlKind.AVG, GremlinSqlAggFunctionImplementations.AVG);
                put(SqlKind.COUNT, GremlinSqlAggFunctionImplementations.COUNT);
                put(SqlKind.SUM, GremlinSqlAggFunctionImplementations.SUM);
                put(SqlKind.SUM0, GremlinSqlAggFunctionImplementations.SUM0);
                put(SqlKind.MIN, GremlinSqlAggFunctionImplementations.MIN);
                put(SqlKind.MAX, GremlinSqlAggFunctionImplementations.MAX);

        /*
        put(SqlKind.LEAD, null);
        put(SqlKind.LAG, null);
        put(SqlKind.FIRST_VALUE, null);
        put(SqlKind.LAST_VALUE, null);
        put(SqlKind.COVAR_POP, null);
        put(SqlKind.COVAR_SAMP, null);
        put(SqlKind.REGR_COUNT, null);
        put(SqlKind.REGR_SXX, null);
        put(SqlKind.REGR_SYY, null);
        put(SqlKind.STDDEV_POP, null);
        put(SqlKind.STDDEV_SAMP, null);
        put(SqlKind.VAR_POP, null);
        put(SqlKind.VAR_SAMP, null);
        put(SqlKind.NTILE, null);
        put(SqlKind.COLLECT, null);
        put(SqlKind.FUSION, null);
        put(SqlKind.SINGLE_VALUE, null);
        put(SqlKind.ROW_NUMBER, null);
        put(SqlKind.RANK, null);
        put(SqlKind.PERCENT_RANK, null);
        put(SqlKind.DENSE_RANK, null);
        put(SqlKind.CUME_DIST, null);
        put(SqlKind.JSON_ARRAYAGG, null);
        put(SqlKind.JSON_OBJECTAGG, null);
        put(SqlKind.BIT_AND, null);
        put(SqlKind.BIT_OR, null);
        put(SqlKind.BIT_XOR, null);
        put(SqlKind.LISTAGG, null);
        put(SqlKind.INTERSECTION, null);
        put(SqlKind.ANY_VALUE, null);
         */
            }};
    SqlAggFunction sqlAggFunction;
    SqlMetadata sqlMetadata;
    List<GremlinSqlNode> sqlOperands;


    public GremlinSqlAggFunction(final SqlAggFunction sqlOperator,
                                 final List<GremlinSqlNode> gremlinSqlNodes,
                                 final SqlMetadata sqlMetadata) {
        super(sqlOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlAggFunction = sqlOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraveral) throws SQLException {
        if (AGGREGATE_APPENDERS.containsKey(sqlAggFunction.kind)) {
            AGGREGATE_APPENDERS.get(sqlAggFunction.kind).appendTraversal(graphTraveral, sqlOperands);
        } else {
            throw new SQLException(
                    String.format("Error: Aggregate function %s is not supported.", sqlAggFunction.kind.sql));
        }
    }

    private static class GremlinSqlAggFunctionImplementations {
        public static GremlinSqlTraversalAppender AVG =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                    graphTraversal.mean();
                };
        public static GremlinSqlTraversalAppender COUNT =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                };
        public static GremlinSqlTraversalAppender SUM =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                };
        public static GremlinSqlTraversalAppender MIN =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                };
        public static GremlinSqlTraversalAppender MAX =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                };

        // TODO: What is SUM0 vs SUM?
        public static GremlinSqlTraversalAppender SUM0 =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                };
    }
}