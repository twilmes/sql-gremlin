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

package org.twilmes.sql.gremlin.util;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.structure.T.label;

/**
 * Created by twilmes on 12/7/15.
 */
public class SqlFactory {

    public static Graph createSpaceGraph() {
        return createSpaceGraph(TinkerGraph.open());
    }

    public static Graph createSpaceGraph(Graph graph) {
        // companies
        Vertex acmeSpaceCo = graph.addVertex(label, "company", "name", "Acme Space");
        Vertex newFrontiers = graph.addVertex(label, "company", "name", "New Frontiers");
        Vertex tomorrowUnlimited = graph.addVertex(label, "company", "name", "Tomorrow Unlimited");
        Vertex spaceTruckers = graph.addVertex(label, "company", "name", "Space Truckers");
        Vertex bankruptCo = graph.addVertex(label, "company", "name", "Bankrupt Co.");

        // planets
        Vertex earth = graph.addVertex(label, "planet", "name", "earth");
        Vertex mars = graph.addVertex(label, "planet", "name", "mars");
        Vertex saturn = graph.addVertex(label, "planet", "name", "saturn");
        Vertex jupiter = graph.addVertex(label, "planet", "name", "jupiter");

        // astronauts
        Vertex tom = graph.addVertex(label, "person", "name", "Tom", "age", 35);
        Vertex patty = graph.addVertex(label, "person", "name", "Patty", "age", 29);
        Vertex phil = graph.addVertex(label, "person", "name", "Phil", "age", 30);
        Vertex susan = graph.addVertex(label, "person", "name", "Susan", "age", 45);
        Vertex juanita = graph.addVertex(label, "person", "name", "Juanita", "age", 50);
        Vertex pavel = graph.addVertex(label, "person", "name", "Pavel", "age", 30);

        // spaceships
        Vertex spaceship1 = graph.addVertex(label, "spaceship", "name", "Ship 1", "model", "delta 1");
        Vertex spaceship2 = graph.addVertex(label, "spaceship", "name", "Ship 2", "model", "delta 1");
        Vertex spaceship3 = graph.addVertex(label, "spaceship", "name", "Ship 3", "model", "delta 2");
        Vertex spaceship4 = graph.addVertex(label, "spaceship", "name", "Ship 4", "model", "delta 3");

        // satellite
        Vertex satellite1 = graph.addVertex(label, "satellite", "name", "sat1");
        Vertex satellite2 = graph.addVertex(label, "satellite", "name", "sat2");
        Vertex satellite3 = graph.addVertex(label, "satellite", "name", "sat3");

        // rocket fuel
        Vertex s1Fuel = graph.addVertex(label, "sensor", "type", "rocket fuel");
//        Vertex s2Fuel = graph.addVertex(label, "sensor", "type", "rocket fuel");
//        Vertex s3Fuel = graph.addVertex(label, "sensor", "type", "rocket fuel");
//        Vertex s4Fuel = graph.addVertex(label, "sensor", "type", "rocket fuel");

        // astronaut company relationships
        tom.addEdge("worksFor", acmeSpaceCo, "yearsWorked", 5);
        patty.addEdge("worksFor", acmeSpaceCo, "yearsWorked", 1);
        phil.addEdge("worksFor", newFrontiers, "yearsWorked", 9);
        susan.addEdge("worksFor", tomorrowUnlimited, "yearsWorked", 4);
        juanita.addEdge("worksFor", spaceTruckers, "yearsWorked", 4);
        pavel.addEdge("worksFor", spaceTruckers, "yearsWorked", 10);

        // astronaut spaceship
        tom.addEdge("pilots", spaceship1);
        patty.addEdge("pilots", spaceship1);
        phil.addEdge("pilots", spaceship2);
        susan.addEdge("pilots", spaceship3);
        juanita.addEdge("pilots", spaceship4);
        pavel.addEdge("pilots", spaceship4);

        // astronauts to planets
        tom.addEdge("fliesTo", earth).property("trips", 10);
        tom.addEdge("fliesTo", mars).property("trips", 3);
        patty.addEdge("fliesTo", mars).property("trips", 1);
        phil.addEdge("fliesTo", saturn).property("trips", 9);
        phil.addEdge("fliesTo", earth).property("trips", 4);
        susan.addEdge("fliesTo", jupiter).property("trips", 20);
        juanita.addEdge("fliesTo", earth).property("trips", 4);
        juanita.addEdge("fliesTo", saturn).property("trips", 7);
        juanita.addEdge("fliesTo", jupiter).property("trips", 9);
        pavel.addEdge("fliesTo", mars).property("trips", 0);

        // astronaut friends
        tom.addEdge("friendsWith", patty);
        patty.addEdge("friendsWith", juanita);
        phil.addEdge("friendsWith", susan);
        susan.addEdge("friendsWith", pavel);

        // satellites to planets
        satellite1.addEdge("orbits", earth).property("launched", 1995);
        satellite2.addEdge("orbits", mars).property("launched", 2020);
        satellite3.addEdge("orbits", jupiter).property("launched", 2005);

        // fuel sensor readings
        long timestamp = 1765258774000L;
        for(int i = 0; i < 10; i++) {
            Vertex s1Reading = graph.addVertex(label, "sensorReading", "timestamp", timestamp, "date", timestamp, "value", 10.0);
            s1Fuel.addEdge("hasReading", s1Reading);
            timestamp += TimeUnit.MINUTES.toMillis(5);
        }

        return graph;
    }
}
