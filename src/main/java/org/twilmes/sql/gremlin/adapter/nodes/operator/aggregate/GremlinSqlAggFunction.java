package org.twilmes.sql.gremlin.adapter.nodes.operator.aggregate;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.GremlinSqlMetadata;
import org.twilmes.sql.gremlin.adapter.GremlinSqlTraversalEngine;
import org.twilmes.sql.gremlin.adapter.nodes.operands.GremlinSqlOperand;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlOperator;
import org.twilmes.sql.gremlin.adapter.nodes.operator.GremlinSqlTraversalAppender;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class GremlinSqlAggFunction extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlOperand.class);
    // See SqlKind.AGGREGATE for list of aggregate functions in Calcite.
    private static final Map<SqlKind, GremlinSqlTraversalAppender> AGGREGATE_APPENDERS = new HashMap<SqlKind, GremlinSqlTraversalAppender>() {{
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
    GremlinSqlMetadata gremlinSqlMetadata;
    List<SqlNode> sqlOperands;


    public GremlinSqlAggFunction(final SqlAggFunction sqlOperator,
                                 final List<SqlNode> sqlOperands,
                                 final GremlinSqlMetadata gremlinSqlMetadata) {
        super(sqlOperator, gremlinSqlMetadata);
        this.sqlAggFunction = sqlOperator;
        this.gremlinSqlMetadata = gremlinSqlMetadata;
        this.sqlOperands = sqlOperands;
    }

    @Override
    public void appendTraversal(GraphTraversal<?, ?> graphTraveral) throws SQLException {
        if (sqlOperands.size() == 1) {
            if (sqlOperands.get(0) instanceof SqlBasicCall) {

            }
        }
        if (AGGREGATE_APPENDERS.containsKey(sqlAggFunction.kind)) {
            AGGREGATE_APPENDERS.get(sqlAggFunction.kind).appendTraversal(graphTraveral, sqlOperands);
        } else {
            throw new SQLException("Error: Aggregate function {} is not supported.", sqlAggFunction.kind.sql);
        }
    }

    private static class GremlinSqlAggFunctionImplementations {
        public static GremlinSqlTraversalAppender AVG = (GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands) ->  {};
        public static GremlinSqlTraversalAppender COUNT = (GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands) ->  {};
        public static GremlinSqlTraversalAppender SUM = (GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands) ->  {};
        public static GremlinSqlTraversalAppender MIN = (GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands) ->  {};
        public static GremlinSqlTraversalAppender MAX = (GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands) ->  {};

        // TODO: What is SUM0 vs SUM?
        public static GremlinSqlTraversalAppender SUM0 = (GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands) ->  {};
    }
}