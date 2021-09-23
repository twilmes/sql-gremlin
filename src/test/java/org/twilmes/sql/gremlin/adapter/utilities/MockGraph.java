package org.twilmes.sql.gremlin.adapter.utilities;

import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableConfig;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import java.util.ArrayList;
import java.util.List;

public interface MockGraph {
    SchemaConfig getSchema();
    List<String> getBasicSelectQueries();
}
