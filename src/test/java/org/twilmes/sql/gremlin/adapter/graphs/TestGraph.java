package org.twilmes.sql.gremlin.adapter.graphs;

import org.apache.tinkerpop.gremlin.structure.Graph;

public interface TestGraph {
    void populate(Graph graph);
}
