package org.twilmes.sql.gremlin.adapter.nodes.operator;

import org.apache.calcite.sql.SqlNode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import java.util.List;

public interface GremlinSqlTraversalAppender {
    void appendTraversal(GraphTraversal<?, ?> graphTraversal, List<SqlNode> operands);
}
