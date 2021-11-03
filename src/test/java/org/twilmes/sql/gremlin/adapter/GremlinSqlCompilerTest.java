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

import org.junit.Before;
import org.junit.Test;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by twilmes on 12/7/15.
 */
public class GremlinSqlCompilerTest extends GremlinSqlBaseTest {

    @Before
    public void setUp() throws SQLException {
        setUpBefore(Data.SPACE);
    }

    void runQueryTestResults(final String query, final List<String> columnNames, final List<List<Object>> rows)
            throws SQLException {
        final SqlGremlinTestResult result = executeQuery(query);
        assertRows(result.getRows(), rows);
        assertColumns(result.getColumns(), columnNames);
    }

    @Test
    public void testProject() throws SQLException {
        runQueryTestResults("select name from person", columns("name"),
                rows(r("Tom"), r("Patty"), r("Phil"), r("Susan"), r("Juanita"), r("Pavel")));
    }

    @Test
    public void testAggregateFunctions() throws SQLException {
        runQueryTestResults("select count(age), min(age), max(age), avg(age) from person",
                columns("COUNT(age)", "MIN(age)", "MAX(age)", "AVG(age)"),
                rows(r(6L, 29, 50, 36)));
    }
    /*

    @Test
    public void testSimpleSelect() {
        // todo fk ids will be added in for these simple selects
        assertResults(query("select * from company"),
                rows(
                        r(null, null, 0L, "Acme Space"),
                        r(null, null, 2L, "New Frontiers"),
                        r(null, null, 4L, "Tomorrow Unlimited"),
                        r(null, null, 6L, "Space Truckers"),
                        r(null, null, 8L, "Bankrupt Co.")
                ));
    }

    @Test
    public void testFiltering() {
        assertResults(query("select company_id, name from company where name = 'Acme Space'"),
                rows(r(0L, "Acme Space")));

        assertResults(query("select * from company where name = 'I do not exist Co.'"),
                rows());

        assertResults(query("select company_id, name from company where name in ('Acme Space', 'Space Truckers')"),
                rows(
                        r(0L, "Acme Space"),
                        r(6L, "Space Truckers")
                ));

        assertResults(query("select name from person where age = 35"),
                rows(r("Tom")));

        assertResults(query("select name from person where age > 35"),
                rows(
                        r("Susan"),
                        r("Juanita")
                ));

        assertResults(query("select name from person where age >= 35"),
                rows(
                        r("Tom"),
                        r("Susan"),
                        r("Juanita")
                ));

        assertResults(query("select name from person where age < 35"),
                rows(
                        r("Patty"),
                        r("Phil"),
                        r("Pavel")
                ));

        assertResults(query("select name from person where age <= 35"),
                rows(
                        r("Tom"),
                        r("Patty"),
                        r("Phil"),
                        r("Pavel")
                ));

        assertResults(query("select name from person where age <> 35"),
                rows(
                        r("Patty"),
                        r("Phil"),
                        r("Susan"),
                        r("Juanita"),
                        r("Pavel")
                ));
    }

    @Test
    public void testInnerJoin() {
        assertResults(query(
                "select c.name, p.name from company as c " +
                        "inner join person as p on p.company_id = c.company_id"),
                rows(
                        r("Acme Space", "Tom"),
                        r("Acme Space", "Patty"),
                        r("New Frontiers", "Phil"),
                        r("Tomorrow Unlimited", "Susan"),
                        r("Space Truckers", "Juanita"),
                        r("Space Truckers", "Pavel")
                ));

        assertResults(query(
                "select c.name, p.name, s.name from company as c " +
                        "inner join person as p on p.company_id = c.company_id " +
                        "inner join spaceship as s on s.spaceship_id = p.spaceship_id"),
                rows(
                        r("Acme Space", "Tom", "Ship 1"),
                        r("Acme Space", "Patty", "Ship 1"),
                        r("New Frontiers", "Phil", "Ship 3"),
                        r("Tomorrow Unlimited", "Susan", "Ship 3"),
                        r("Space Truckers", "Juanita", "Ship 4"),
                        r("Space Truckers", "Pavel", "Ship 4")
                ));

        // test that primary keys come back correctly
        assertResults(query(
                "select c.company_id, p.person_id, p.company_id, p.spaceship_id, s.spaceship_id " +
                        "from company as c " +
                        "inner join person as p on p.company_id = c.company_id " +
                        "inner join spaceship as s on s.spaceship_id = p.spaceship_id"),
                rows(
                        r(0L, 18L, 0L, 36L, 36L),
                        r(0L, 21L, 0L, 36L, 36L),
                        r(2L, 24L, 2L, 39L, 39L),
                        r(4L, 27L, 4L, 42L, 42L),
                        r(6L, 30L, 6L, 45L, 45L),
                        r(6L, 33L, 6L, 45L, 45L)
                ));
    }

    @Test
    public void testInnerJoinFilters() {
        assertResults(query(
                "select c.name, p.name from company as c " +
                        "inner join person as p on p.company_id = c.company_id " +
                        "where c.name = 'Acme Space'"),
                rows(
                        r("Acme Space", "Tom"),
                        r("Acme Space", "Patty")
                ));

        assertResults(query(
                "select c.name, p.name, s.name from company as c " +
                        "inner join person as p on p.company_id = c.company_id " +
                        "inner join spaceship as s on s.spaceship_id = p.spaceship_id " +
                        "where c.name = 'Space Truckers' and s.name = 'Ship 4' and p.name = 'Juanita' "),
                rows(
                        r("Space Truckers", "Juanita", "Ship 4")
                ));
    }

    @Test
    public void testSelfJoin() {
        assertResults(query("select p1.name, p2.name from person as p1 " +
                        "inner join person as p2 on p1.person_id = p2.person_id"),
                rows(
                        r("Tom", "Patty"),
                        r("Patty", "Juanita"),
                        r("Phil", "Susan"),
                        r("Susan", "Pavel")
                ));

        // friends of a friend
        assertResults(query("select p1.name, p2.name, p3.name from person as p1 " +
                        "inner join person as p2 on p1.person_id = p2.person_id " +
                        "inner join person as p3 on p3.person_id = p2.person_id"),
                rows(
                        r("Juanita", "Patty", "Tom"),
                        r("Pavel", "Susan", "Phil")
                ));

        // friends of a friend's spaceship
        assertResults(query("select p1.name, p2.name, p3.name, s.name from person as p1 " +
                        "inner join person as p2 on p1.person_id = p2.person_id " +
                        "inner join person as p3 on p3.person_id = p2.person_id " +
                        "inner join spaceship as s on s.spaceship_id = p3.spaceship_id"),
                rows(
                        r("Tom", "Patty", "Juanita", "Ship 4"),
                        r("Phil", "Susan", "Pavel", "Ship 4")
                ));
    }

    @Test
    public void testAssocs() {
        assertResults(query("select p.name, pl.name, fliesto.trips from person as p " +
                        "inner join fliesto on fliesto.person_id = p.person_id " +
                        "inner join planet as pl on pl.planet_id = fliesto.planet_id"),
                rows(
                        r("Tom", "earth", 10),
                        r("Tom", "mars", 3),
                        r("Patty", "mars", 1),
                        r("Phil", "saturn", 9),
                        r("Phil", "earth", 4),
                        r("Susan", "jupiter", 20),
                        r("Juanita", "earth", 4),
                        r("Juanita", "saturn", 7),
                        r("Juanita", "jupiter", 9),
                        r("Pavel", "mars", 0)
                ));

        assertResults(query("select p.name, pl.name, fliesto.trips, sat.name from person as p " +
                        "inner join fliesto on fliesto.person_id = p.person_id " +
                        "inner join planet as pl on pl.planet_id = fliesto.planet_id " +
                        "inner join orbits on orbits.planet_id = pl.planet_id " +
                        "inner join satellite as sat on sat.satellite_id = orbits.satellite_id"),
                rows(
                        r("Tom", "earth", 10, "sat1"),
                        r("Tom", "mars", 3, "sat2"),
                        r("Patty", "mars", 1, "sat2"),
                        r("Phil", "earth", 4, "sat1"),
                        r("Susan", "jupiter", 20, "sat3"),
                        r("Juanita", "earth", 4, "sat1"),
                        r("Juanita", "jupiter", 9, "sat3"),
                        r("Pavel", "mars", 0, "sat2")
                ));

        // reorder joins
        assertResults(query("select p.name, pl.name, fliesto.trips, sat.name from planet as pl " +
                        "inner join orbits on orbits.planet_id = pl.planet_id " +
                        "inner join satellite as sat on sat.satellite_id = orbits.satellite_id " +
                        "inner join fliesto on fliesto.planet_id = pl.planet_id " +
                        "inner join person as p on p.person_id = fliesto.person_id"),
                rows(
                        r("Tom", "earth", 10, "sat1"),
                        r("Tom", "mars", 3, "sat2"),
                        r("Patty", "mars", 1, "sat2"),
                        r("Phil", "earth", 4, "sat1"),
                        r("Susan", "jupiter", 20, "sat3"),
                        r("Juanita", "earth", 4, "sat1"),
                        r("Juanita", "jupiter", 9, "sat3"),
                        r("Pavel", "mars", 0, "sat2")
                ));
    }

    @Test
    public void testGroupBy() {
        assertResults(query("select c.name, s.model, count(*) from company as c " +
                        "inner join person as p on p.company_id = c.company_id " +
                        "inner join spaceship as s on s.spaceship_id = p.spaceship_id " +
                        "group by c.name, s.model"),
                rows(
                        r("Tomorrow Unlimited", "delta 2", 1L),
                        r("New Frontiers", "delta 1", 1L),
                        r("Acme Space", "delta 1", 2L),
                        r("Space Truckers", "delta 3", 2L)
                ));

        assertResults(query("select c.name, s.model, count(*) from company as c " +
                        "inner join person as p on p.company_id = c.company_id " +
                        "inner join spaceship as s on s.spaceship_id = p.spaceship_id " +
                        "group by c.name, s.model having count(*) > 1"),
                rows(
                        r("Acme Space", "delta 1", 2L),
                        r("Space Truckers", "delta 3", 2L)
                ));
    }

     */
}