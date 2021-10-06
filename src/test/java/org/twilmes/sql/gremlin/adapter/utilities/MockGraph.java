package org.twilmes.sql.gremlin.adapter.utilities;

import org.twilmes.sql.gremlin.adapter.converter.schema.SchemaConfig;
import java.util.List;

public interface MockGraph {
    SchemaConfig getSchema();
    List<String> getBasicSelectQueries();
    List<String> getAggSelectQueries();
    List<String> getJoinQueries();
}
