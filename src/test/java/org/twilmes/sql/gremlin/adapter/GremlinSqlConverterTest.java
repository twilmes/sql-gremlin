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

package org.twilmes.sql.gremlin.adapter;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.SqlConverter;
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
        final SqlConverter converter = new SqlConverter(mockGraph.getSchema(), g);
        // converter.getStringTraversal("SELECT name FROM Person AS person");
        for (final String query : mockGraph.getBasicSelectQueries()) {
            LOGGER.info("Running query \"{}\".", query);
            System.out.println("String traversal: \"" + converter.getStringTraversal(query) + "\"");
        }
        for (final String query : mockGraph.getAggSelectQueries()) {
            LOGGER.info("Running query \"{}\".", query);
            // System.out.println("String traversal: \"" + converter.getStringTraversal(query) + "\"");
        }
        for (final String query : mockGraph.getJoinQueries()) {
            LOGGER.info("Running query \"{}\".", query);
            // System.out.println("String traversal: \"" + converter.getStringTraversal(query) + "\"");
        }
    }
}
