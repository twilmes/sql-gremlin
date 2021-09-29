package org.twilmes.sql.gremlin.adapter.utilities;

import com.google.common.collect.ImmutableList;
import org.twilmes.sql.gremlin.adapter.converter.schema.SchemaConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableColumn;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableRelationship;
import java.util.ArrayList;
import java.util.List;

public class MockPersonGraph implements MockGraph {
    public static final SchemaConfig PERSON_GRAPH_SCHEMA;
    private static final List<String> BASIC_SELECT_QUERIES = ImmutableList.of(
            /*
            "SELECT name FROM Person",
            "SELECT name FROM person",
            "SELECT NAME FROM person",
            "SELECT NaMe FROM Person",
            "SELECT NaMe FROM pERson",
            "SELECT `person`.`name` FROM person",
            "SELECT `person`.`name` AS `n` FROM `person`",
            "SELECT `p`.`name` FROM `person` as `p`",
            "SELECT `p`.`name` AS `n` FROM `person` as `p`",
            "SELECT `p`.`name` AS `n`, `p`.`name` AS `name` FROM `person` as `p`",
            "SELECT `p`.`name` AS `n`, p.name AS name FROM `person` as `p`",
            "SELECT `p`.`name` AS `n`, p.age AS age FROM `person` as `p`",
            "SELECT `p`.`name` AS `n`, p.age AS age FROM `person` as `p`",*/
            "SELECT `p`.`name` AS `n`, p.age AS age, p.name AS name FROM `person` as `p` GROUP BY name, age LIMIT 1000");
    private static final List<String> AGG_FUNCTIONS = ImmutableList.of("AVG");
    private static final List<String> JOIN_QUERIES = ImmutableList.of(
            "SELECT `person1`.`name` AS `name1`, `person2`.`name` AS `name2` FROM `gremlin`.`Person` `person1` INNER JOIN "
            + "`gremlin`.`Person` `person2` ON (`person1`.`KNOWS_ID` = `person2`.`KNOWS_ID`) "
                    + "GROUP BY `person1`.`name`, `person2`.`name`");
    private static final List<String> BASIC_SELECT_AGG_QUERIES = ImmutableList.of(
            "SELECT age AS a FROM Person",
            "SELECT age FROM Person",
            "SELECT %s(age) AS a FROM Person",
            "SELECT %s(age) FROM Person",
            "SELECT %s(age) FROM Person",
            "SELECT %s(`person`.`age`) AS `age`, `person`.`age` AS `age` FROM `person` G.P BY `person`.`age`");

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
        personColumns.add(new TableColumn("KNOWS_ID", "string", null));
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

    public List<String> getJoinQueries() {
        return JOIN_QUERIES;
    }

    public List<String> getAggSelectQueries() {
        final List<String> aggFunctions = new ArrayList<>();
        for (final String query : BASIC_SELECT_AGG_QUERIES) {
            for (final String aggFunction : AGG_FUNCTIONS) {
                aggFunctions.add(String.format(query, aggFunction));
            }
        }
        return aggFunctions;
    }
}
