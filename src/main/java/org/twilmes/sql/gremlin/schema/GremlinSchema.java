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
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Created by twilmes on 9/22/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinSchema extends AbstractSchema {
    private final SchemaConfig config;
    private Map<String, Table> schema = null;

    public GremlinSchema(final SchemaConfig config) {
        this.config = config;
    }

    // TODO: Fix this to better fit the design - this is a hack to get the required info.
    public static TableDef getTableDef(final TableRelationship tableRelationship) {
        final TableDef tableDef =
                new TableDef(tableRelationship.getEdgeLabel(), tableRelationship.getEdgeLabel(), false);

        // Set table columns.
        if (tableRelationship.getColumns() != null) {
            setTableColumns(tableDef, tableRelationship.getColumns());
        }

        // Get primary key for table def.
        final TableColumn pk = getPrimaryKey(tableDef);
        tableDef.columns.put(pk.getName(), pk);

        // Get in and out foreign keys of edge.
        final TableColumn inFk = getForeignKey(tableRelationship.getInTable());
        final TableColumn outFk = getForeignKey(tableRelationship.getOutTable());
        tableDef.columns.put(inFk.getName(), inFk);
        tableDef.columns.put(outFk.getName(), outFk);
        return tableDef;
    }

    // TODO: Fix this to better fit the design - this is a hack to get the required info.
    public static TableDef getTableDef(final TableConfig tableConfig, SchemaConfig config) {
        final TableDef tableDef = new TableDef(tableConfig.getName(), tableConfig.getName(), true);

        // Set table columns.
        setTableColumns(tableDef, tableConfig.getColumns());

        // Add primary key.
        final TableColumn pk = getPrimaryKey(tableDef);
        tableDef.columns.put(pk.getName(), pk);

        // Get relationship info for vertex.
        final List<TableRelationship> outRelationships = config.getRelationships().
                stream().filter(rel -> rel.getOutTable().equals(tableConfig.getName())).collect(toList());
        final List<TableRelationship> inRelationships = config.getRelationships().
                stream().filter(rel -> rel.getInTable().equals(tableConfig.getName())).collect(toList());

        outRelationships.forEach(rel -> {
            tableDef.hasIn = true;
            final TableColumn fk = getForeignKey(rel);
            tableDef.columns.put(fk.getName(), fk);
        });
        inRelationships.forEach(rel -> {
            tableDef.hasOut = true;
            final TableColumn fk = getForeignKey(rel);
            tableDef.columns.put(fk.getName(), fk);
        });

        return tableDef;
    }

    static void setTableColumns(final TableDef tableDef, final List<TableColumn> tableColumns) {
        tableColumns.forEach(column -> tableDef.columns.put(column.getName().toUpperCase(), column));
    }

    static TableColumn getPrimaryKey(final TableDef tableDef) {
        final TableColumn pk = new TableColumn();
        pk.setName(tableDef.label.toUpperCase() + "_ID");
        pk.setType(tableDef.isVertex ? "long" : "string");
        return pk;
    }

    static TableColumn getForeignKey(final TableRelationship tableRelationship) {
        final TableColumn fk = new TableColumn();
        final String fkName = tableRelationship.getEdgeLabel().toUpperCase() + "_ID";
        fk.setName(fkName);
        fk.setType("long");
        return fk;
    }

    static TableColumn getForeignKey(final String vertexLabel) {
        final TableColumn fk = new TableColumn();
        final String fkName = vertexLabel.toUpperCase() + "_ID";
        fk.setName(fkName);
        fk.setType("long");
        return fk;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (schema == null) {
            schema = buildSchemaFromConfig();
        }
        return schema;
    }

    private Map<String, Table> buildSchemaFromConfig() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        builder.putAll(
                config.getTables().stream().
                        collect(Collectors.toMap(t -> t.getName().toUpperCase(), this::handleTableConfig)));
        builder.putAll(
                config.getRelationships().stream()
                        .collect(Collectors.toMap(r -> r.getEdgeLabel().toUpperCase(), this::handleTableRelationship)));
        return builder.build();
    }

    GremlinTable handleTableRelationship(final TableRelationship tableRelationship) {
        final TableDef tableDef =
                new TableDef(tableRelationship.getEdgeLabel(), tableRelationship.getEdgeLabel(), false);
        final GremlinTable gremlinTable = new GremlinTable(tableDef);

        // Set table columns.
        if (tableRelationship.getColumns() != null) {
            setTableColumns(tableDef, tableRelationship.getColumns());
        }

        // Get primary key for table def.
        final TableColumn pk = getPrimaryKey(tableDef);
        tableDef.columns.put(pk.getName(), pk);

        // Get in and out foreign keys of edge.
        final TableColumn inFk = getForeignKey(tableRelationship.getInTable());
        final TableColumn outFk = getForeignKey(tableRelationship.getOutTable());
        tableDef.columns.put(inFk.getName(), inFk);
        tableDef.columns.put(outFk.getName(), outFk);

        return gremlinTable;
    }

    GremlinTable handleTableConfig(final TableConfig tableConfig) {
        final TableDef tableDef = new TableDef(tableConfig.getName(), tableConfig.getName(), true);
        final GremlinTable gremlinTable = new GremlinTable(tableDef);

        // Set table columns.
        setTableColumns(tableDef, tableConfig.getColumns());

        // Add primary key.
        final TableColumn pk = getPrimaryKey(tableDef);
        tableDef.columns.put(pk.getName(), pk);

        // Get relationship info for vertex.
        final List<TableRelationship> outRelationships = config.getRelationships().
                stream().filter(rel -> rel.getOutTable().equals(tableConfig.getName())).collect(toList());
        final List<TableRelationship> inRelationships = config.getRelationships().
                stream().filter(rel -> rel.getInTable().equals(tableConfig.getName())).collect(toList());

        outRelationships.forEach(rel -> {
            tableDef.hasIn = true;
            final TableColumn fk = getForeignKey(rel);
            tableDef.columns.put(fk.getName(), fk);
        });
        inRelationships.forEach(rel -> {
            tableDef.hasOut = true;
            final TableColumn fk = getForeignKey(rel);
            tableDef.columns.put(fk.getName(), fk);
        });
        return gremlinTable;
    }
}
