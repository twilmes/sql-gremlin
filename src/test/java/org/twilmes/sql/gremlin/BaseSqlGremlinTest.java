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

package org.twilmes.sql.gremlin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.twilmes.sql.gremlin.processor.GremlinCompiler;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.util.SqlFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Created by twilmes on 12/4/15.
 */
public class BaseSqlGremlinTest {
    protected Graph graph;
    protected GraphTraversalSource g;
    private GremlinCompiler compiler;

    private SchemaConfig schemaConfig;

    protected enum Data {
        MODERN,
        CLASSIC,
        CREW,
        NORTHWIND,
        SPACE
    }

    public void setUpBefore(Data data) {
        switch(data) {
            case MODERN:
                graph = TinkerFactory.createModern();
                break;
            case CLASSIC:
                graph = TinkerFactory.createClassic();
                break;
            case CREW:
                graph = TinkerFactory.createTheCrew();
                break;
            case SPACE:
                graph = SqlFactory.createSpaceGraph();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported graph " + data);
        }
        g = graph.traversal();

        ObjectMapper mapper = new ObjectMapper();
        try {
            schemaConfig = mapper.readValue(new File("schema.json"), SchemaConfig.class);
        } catch (Exception e) {
            throw new SchemaException("Error reading the schema file.", e);
        }

        compiler = new GremlinCompiler(graph, schemaConfig);

    }

    public List<Object> query(String sql) {
        List<Object> rows = compiler.execute(sql);

        System.out.println(sql);
        rows.stream().forEach(row -> {
            if(row instanceof Object[]) {
                System.out.println(Arrays.toString((Object[])row));
            } else {
                System.out.println(row);
            }
        });

        return rows;
    }

    public List<Object> rows(Object... rows) {
        List<Object> results = new ArrayList<>();
        for(Object row : rows) {
            results.add(row);
        }
        return results;
    }

    public Object r(Object... columns) {
        if(columns.length == 1) {
            return columns[0];
        }
        return columns;
    }

    public void assertResults(List<Object> l1, List<Object> l2) {
        assertEquals(l1.size(), l2.size());
        for (int i = 0; i < l1.size(); i++) {
            Object e1 = l1.get(0);
            Object e2 = l2.get(0);
            if (e1 instanceof Object[]) {
                assertArrayEquals((Object[]) e1, (Object[]) e2);
            } else {
                assertEquals(e1, e2);
            }
        }
    }
}
