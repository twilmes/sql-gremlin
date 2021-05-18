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

package org.twilmes.sql.gremlin.schema;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.twilmes.sql.gremlin.rel.GremlinTable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

/**
 * Created by twilmes on 9/22/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinSchema extends AbstractSchema {
    private final SchemaConfig config;

    public GremlinSchema(final SchemaConfig config) {
        this.config = config;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return buildSchemaFromConfig();
    }

    private Map<String, Table> buildSchemaFromConfig() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (final TableConfig tableConfig : config.getTables()) {
            final TableDef tableDef = new TableDef();
            tableDef.label = tableConfig.getName();
            tableDef.tableName = tableConfig.getName();

            // is this a vertex or edge table?
            TableRelationship relationship = null;
            final Optional<TableRelationship> optionalRelationship = config.
                    getRelationships().stream().filter(rel -> rel.getEdgeLabel().equals(tableDef.label)).findFirst();
            if (optionalRelationship.isPresent()) {
                relationship = optionalRelationship.get();
                tableDef.isVertex = false;
            } else {
                tableDef.isVertex = true;
            }

            final GremlinTable gremlinTable = new GremlinTable(tableDef);

            tableConfig.getColumns().forEach(column -> {
                tableDef.columns.put(column.getName().toUpperCase(), column);
            });
            // add pk
            final TableColumn pk = new TableColumn();
            // if this is an edge, it'll be the edge id
            final String pkName = tableDef.label.toUpperCase() + "_ID";
            pk.setName(pkName);
            pk.setType(tableDef.isVertex ? "long" : "string");
            tableDef.columns.put(pkName, pk);

            if (tableDef.isVertex) {
                // get relationship info
                final List<TableRelationship> outRelationships = config.getRelationships().
                        stream().filter(rel -> rel.getOutTable().equals(tableConfig.getName()) &&
                        rel.getFkTable().equals(rel.getOutTable())).collect(toList());
                final List<TableRelationship> inRelationships = config.getRelationships().
                        stream().filter(rel -> rel.getInTable().equals(tableConfig.getName()) &&
                        rel.getFkTable().equals(rel.getInTable())).collect(toList());

                outRelationships.forEach(rel -> {
//                    tableDef.outEdgeMap.put(rel.getEdgeLabel(), rel.getInTable());
                    // add fk
                    final TableColumn fk = new TableColumn();
                    final String fkName;
                    if (rel.getInTable().equals(rel.getOutTable())) {
                        fkName = rel.getEdgeLabel().toUpperCase() + "_ID";
                    } else {
                        fkName = rel.getInTable().toUpperCase() + "_ID";
                    }
                    fk.setName(fkName);
                    fk.setType("long");
                    tableDef.columns.put(fkName, fk);
                });

                inRelationships.forEach(rel -> {
                    // add fk
                    final TableColumn fk = new TableColumn();
                    final String fkName = rel.getOutTable().toUpperCase() + "_ID";
                    fk.setName(fkName);
                    fk.setType("long");
                    tableDef.columns.put(fkName, fk);
                });
            } else {
                // it's an edge so set fks
                final String inTable = relationship.getInTable();
                final String inFkName = inTable.toUpperCase() + "_ID";
                final String outTable = relationship.getOutTable();
                final TableColumn inFk = new TableColumn();
                inFk.setName(inFkName);
                inFk.setType("long");
                final String outFkName = outTable.toUpperCase() + "_ID";
                final TableColumn outFk = new TableColumn();
                outFk.setName(outFkName);
                outFk.setType("long");

                tableDef.columns.put(inFkName, inFk);
                tableDef.columns.put(outFkName, outFk);
            }
            builder.put(tableDef.label.toUpperCase(), gremlinTable);
        }
        return builder.build();
    }
}
