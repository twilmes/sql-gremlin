package org.twilmes.sql.gremlin.adapter.utilities;

import com.google.common.collect.ImmutableList;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableConfig;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import java.util.ArrayList;
import java.util.List;

public class MockPersonGraph implements MockGraph {
    public static final SchemaConfig PERSON_GRAPH_SCHEMA;
    private static final List<String> BASIC_SELECT_QUERIES = ImmutableList.of(
            "SELECT name FROM Person",
            "SELECT name FROM person",
            "SELECT NAME FROM person",
            "SELECT NaMe FROM Person",
            "SELECT NaMe FROM pERson",
            "SELECT `person`.`name` FROM person",
            "SELECT `person`.`name` AS `n` FROM `person`",
            "SELECT `p`.`name` FROM `person` as `p`",
            "SELECT `p`.`name` AS `n` FROM `person` as `p`");
    private static final List<String> AGG_FUNCTIONS = ImmutableList.of("AVG");
    private static final List<String> BASIC_SELECT_AGG_QUERIES = ImmutableList.of(
            "SELECT %s(age) FROM Person",
            "SELECT %s(age), name FROM Person",
            "SELECT %s(age) AS a, name as n FROM Person",
            "SELECT %s(age) AS a, name as n FROM Person",
            "SELECT %s(age) AS a, name as n FROM Person");

    static {
        PERSON_GRAPH_SCHEMA = generatePersonGraphSchema();
    }

    private static SchemaConfig generatePersonGraphSchema() {
        final List<TableConfig> vertices = new ArrayList<>();
        final List<TableRelationship> edges = new ArrayList<>();

        // Node: Person
        // Columns: age, name
        final List<TableColumn> personColumns = new ArrayList<>();
        personColumns.add(new TableColumn("age", "integer", null));
        personColumns.add(new TableColumn("name", "string", null));
        vertices.add(new TableConfig("Person", personColumns));

        // Edge: Knows
        // Columns: from
        final List<TableColumn> knowsColumns = new ArrayList<>();
        personColumns.add(new TableColumn("from", "string", null));
        final TableRelationship knowsRelationship = new TableRelationship();
        knowsRelationship.setEdgeLabel("Knows");
        knowsRelationship.setColumns(knowsColumns);
        knowsRelationship.setInTable("Person");
        knowsRelationship.setOutTable("Person");
        knowsRelationship.setFkTable(null);

        return new SchemaConfig(vertices, edges);
    }

    public SchemaConfig getSchema() {
        return PERSON_GRAPH_SCHEMA;
    }

    public List<String> getBasicSelectQueries() {
        return BASIC_SELECT_QUERIES;
    }
}
