package org.twilmes.sql.gremlin.adapter.utilities;

import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import java.util.List;

public interface MockGraph {
    GremlinSchema getSchema();
    List<String> getBasicSelectQueries();
    List<String> getAggSelectQueries();
    List<String> getJoinQueries();
}
