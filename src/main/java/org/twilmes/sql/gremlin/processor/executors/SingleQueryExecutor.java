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

package org.twilmes.sql.gremlin.processor.executors;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Executes a query that does not have any joins.
 * <p>
 * select * from customer where name = 'Joe'
 * <p>
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class SingleQueryExecutor extends QueryExecutor {
    SchemaConfig schemaConfig;
    TableDef tableDef;

    public SingleQueryExecutor(final SchemaConfig schemaConfig, final SqlToGremlin.GremlinParseInfo gremlinParseInfo,
                               final TableDef tableDef) {
        super(gremlinParseInfo, schemaConfig);
        this.schemaConfig = schemaConfig;
        this.tableDef = tableDef;
    }

    @Override
    public SqlGremlinQueryResult handle(final GraphTraversalSource g) throws SQLException {
        final GraphTraversal<?, ?> traversal =
                (tableDef.isVertex ? g.V() : g.E()).hasLabel(tableDef.label).project(tableDef.label)
                        .by(getSelectTraversal(tableDef));
        imposeLimit(traversal);

        final List<String> fields =
                gremlinParseInfo.getGremlinSelectInfos().stream().map(SqlToGremlin.GremlinSelectInfo::getMappedName)
                        .collect(Collectors.toList());
        final SqlGremlinQueryResult sqlGremlinQueryResult =
                new SqlGremlinQueryResult(fields, ImmutableList.of(tableDef));

        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());

        executor.execute(new Pagination(new SimpleDataReader(tableDef), traversal, sqlGremlinQueryResult));
        executor.shutdown();

        return sqlGremlinQueryResult;
    }

    class SimpleDataReader implements GetRowFromMap {
        final String tableName;
        final List<String> columnNames;

        SimpleDataReader(final TableDef tableDef) {
            tableName = tableDef.label;
            columnNames = gremlinParseInfo.getGremlinSelectInfos().stream()
                    .map(s -> tableDef.getColumn(s.getColumn()).getName()).collect(Collectors.toList());
        }

        @Override
        public Object[] execute(final Map<String, Object> map) {
            final Object[] row = new Object[columnNames.size()];
            int i = 0;
            for (final String column : columnNames) {
                final Optional<String> tableKey =
                        map.keySet().stream().filter(key -> key.equalsIgnoreCase(tableName)).findFirst();
                if (!tableKey.isPresent()) {
                    row[i++] = null;
                    continue;
                }

                final Optional<String> columnKey = ((Map<String, Object>) map.get(tableKey.get())).keySet().stream()
                        .filter(key -> key.equalsIgnoreCase(column)).findFirst();
                if (!columnKey.isPresent()) {
                    row[i++] = null;
                    continue;
                }
                row[i++] = ((Map<String, Object>) map.get(tableKey.get())).getOrDefault(columnKey.get(), null);
            }
            return row;
        }
    }
}
