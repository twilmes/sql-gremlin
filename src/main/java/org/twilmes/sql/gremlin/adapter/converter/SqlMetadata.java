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

package org.twilmes.sql.gremlin.adapter.converter;

import lombok.Getter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.schema.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.SchemaConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableColumn;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableConfig;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableDef;
import org.twilmes.sql.gremlin.adapter.converter.schema.TableRelationship;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This module contains traversal and query metadata used by the adapter.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
@Getter
public class SqlMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlMetadata.class);
    private final GraphTraversalSource g;
    private final SchemaConfig schemaConfig;
    private final GremlinSchema gremlinSchema;
    private final Map<String, String> tableRenameMap = new HashMap<>();
    private final Map<String, String> columnRenameMap = new HashMap<>();
    private final Map<String, List<String>> columnOutputListMap = new HashMap<>();
    private boolean isAggregate = false;

    public SqlMetadata(final GraphTraversalSource g, final SchemaConfig schemaConfig) {
        this.g = g;
        this.schemaConfig = schemaConfig;
        gremlinSchema = new GremlinSchema(schemaConfig);
    }

    private static boolean isAggregate(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlCall) {
            final SqlCall sqlCall = (SqlCall) sqlNode;
            if (isAggregate(sqlCall.getOperator())) {
                return true;
            }
            for (final SqlNode tmpSqlNode : sqlCall.getOperandList()) {
                if (isAggregate(tmpSqlNode)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isAggregate(final SqlOperator sqlOperator) {
        return sqlOperator instanceof SqlAggFunction;
    }

    public boolean getIsColumnEdge(final String tableName, final String columnName) throws SQLException {
        return getTableDef(tableName).isVertex && columnName.toUpperCase().endsWith("_ID");
    }

    public String getColumnEdgeLabel(final String column) throws SQLException {
        final String columnName = getRenamedColumn(column);
        if (!columnName.toUpperCase().endsWith("_ID")) {
            throw new SQLException("Error: Edge labels must end with _ID.");
        }
        final TableDef tableDef = getTableDef(column.substring(0, column.length() - "_ID".length()));
        if (tableDef.isVertex) {
            throw new SQLException("Error: Expected edge table.");
        }
        return tableDef.label;
    }

    public boolean isLeftInRightOut(final String leftVertexLabel, final String rightVertexLabel) {
        return schemaConfig.getRelationships().stream().anyMatch(r ->
                r.getInTable().equalsIgnoreCase(leftVertexLabel) && r.getOutTable().equalsIgnoreCase(rightVertexLabel));
    }

    public boolean isRightInLeftOut(final String leftVertexLabel, final String rightVertexLabel) {
        return schemaConfig.getRelationships().stream().anyMatch(r ->
                r.getOutTable().equalsIgnoreCase(leftVertexLabel) && r.getInTable().equalsIgnoreCase(rightVertexLabel));
    }

    public Set<String> getRenamedColumns() {
        return new HashSet<>(columnRenameMap.keySet());
    }

    public void setColumnOutputList(final String table, final List<String> columnOutputList) {
        columnOutputListMap.put(table, new ArrayList<>(columnOutputList));
    }

    public Set<TableDef> getTables() throws SQLException {
        final Set<TableDef> tables = new HashSet<>();
        for (final String table : tableRenameMap.values()) {
            tables.add(getTableDef(table));
        }
        return tables;
    }

    public boolean isVertex(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final TableConfig tableConfig : schemaConfig.getTables()) {
            if (tableConfig.getName().equalsIgnoreCase(renamedTableName)) {
                return true;
            }
        }
        for (final TableRelationship tableRelationship : schemaConfig.getRelationships()) {
            if (tableRelationship.getEdgeLabel().equalsIgnoreCase(renamedTableName)) {
                return false;
            }
        }
        throw new SQLException("Error: Table {} does not exist.", renamedTableName);
    }

    public TableDef getTableDef(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final TableConfig tableConfig : schemaConfig.getTables()) {
            if (tableConfig.getName().equalsIgnoreCase(renamedTableName)) {
                return GremlinSchema.getTableDef(tableConfig, schemaConfig);
            }
        }
        for (final TableRelationship tableRelationship : schemaConfig.getRelationships()) {
            if (tableRelationship.getEdgeLabel().equalsIgnoreCase(renamedTableName)) {
                return GremlinSchema.getTableDef(tableRelationship);
            }
        }
        throw new SQLException(String.format("Error: Table %s does not exist.", renamedTableName));
    }

    public void addRenamedTable(final String actualName, final String renameName) {
        tableRenameMap.put(renameName, actualName);
    }

    public String getRenamedTable(final String table) {
        return tableRenameMap.getOrDefault(table, table);
    }

    public void addRenamedColumn(final String actualName, final String renameName) {
        columnRenameMap.put(renameName, actualName);
    }

    public String getRenamedColumn(final String column) {
        return columnRenameMap.getOrDefault(column, column);
    }

    public String getActualColumnName(final TableDef table, final String column) throws SQLException {
        for (final TableColumn tableColumn : table.columns.values()) {
            if (tableColumn.getName().equalsIgnoreCase(column)) {
                return tableColumn.getName();
            }
        }
        throw new SQLException(String.format("Error: Column %s does not exist in table %s.", column, table.tableName));
    }

    public boolean getTableHasColumn(final TableDef table, final String column) {
        for (final TableColumn tableColumn : table.columns.values()) {
            if (tableColumn.getName().equalsIgnoreCase(column)) {
                return true;
            }
        }
        return false;
    }

    public String getActualTableName(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final TableConfig tableConfig : schemaConfig.getTables()) {
            if (tableConfig.getName().equalsIgnoreCase(renamedTableName)) {
                return tableConfig.getName();
            }
        }
        for (final TableRelationship tableRelationship : schemaConfig.getRelationships()) {
            if (tableRelationship.getEdgeLabel().equalsIgnoreCase(renamedTableName)) {
                return tableRelationship.getEdgeLabel();
            }
        }
        throw new SQLException(String.format("Error: Table %s.", table));
    }

    public void checkAggregate(final SqlNodeList sqlNodeList) {
        isAggregate = sqlNodeList.getList().stream().anyMatch(SqlMetadata::isAggregate);
    }

    public boolean getIsAggregate() {
        return isAggregate;
    }
}
