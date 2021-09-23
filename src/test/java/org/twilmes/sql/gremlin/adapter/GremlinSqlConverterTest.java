package org.twilmes.sql.gremlin.adapter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.utilities.MockGraph;
import org.twilmes.sql.gremlin.adapter.utilities.MockPersonGraph;
import java.sql.SQLException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class GremlinSqlConverterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlConverterTest.class);
    private static final MockGraph PERSON_GRAPH = new MockPersonGraph();
    private final GraphTraversalSource g = traversal().withGraph(EmptyGraph.instance());

    @Test
    void testBasicSelectQueriesPersonGraph() throws SQLException {
        runQueriesForGraph(PERSON_GRAPH);
    }

    void runQueriesForGraph(final MockGraph mockGraph) throws SQLException {
        final GremlinSqlConverter converter = new GremlinSqlConverter(mockGraph.getSchema(), g);
        for (final String query : mockGraph.getBasicSelectQueries()) {
            LOGGER.info("Running query \"{}\".", query);
            System.out.println("String traversal: \"" + converter.getStringTraversal(query) + "\"");
        }
    }
}
