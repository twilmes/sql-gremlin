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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.visitors.JoinVisitor;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.id;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;

@AllArgsConstructor
public class QueryExecutor {
    protected static final String IN_V_ID_KEY = "__in_v_id__";
    protected static final String OUT_V_ID_KEY = "__out_v_id__";
    protected static final String IN_V_LABEL_KEY = "__in_v_label__";
    protected static final String OUT_V_LABEL_KEY = "__out_v_label__";
    protected static final String IN_E_ID_KEY = "__in_e_id__";
    protected static final String OUT_E_ID_KEY = "__out_e_id__";
    protected static final String IN_E_LABEL_KEY = "__in_e_label__";
    protected static final String OUT_E_LABEL_KEY = "__out_e_label__";
    protected static final String MAP_KEY = "__map__";
    protected static final String ID_KEY = "__id__";
    protected final SqlToGremlin.GremlinParseInfo gremlinParseInfo;

    protected static void appendGetAllVertexResultsProject(final TableDef tableDef,
                                                           final GraphTraversal<?, ?> traversal) {
        final List<String> additionalKeys = new ArrayList<>();
        additionalKeys.add(ID_KEY);
        if (tableDef.hasIn) {
            additionalKeys.add(IN_E_ID_KEY);
            additionalKeys.add(IN_E_LABEL_KEY);
        }
        if (tableDef.hasOut) {
            additionalKeys.add(OUT_E_ID_KEY);
            additionalKeys.add(OUT_E_LABEL_KEY);
        }

        traversal.project(MAP_KEY, additionalKeys.toArray(new String[0]))
                .by(valueMap().with(WithOptions.tokens))
                .by(id());
        if (tableDef.hasIn) {
            traversal.by(inE().id())
                    .by(inE().label());
        }
        if (tableDef.hasOut) {
            traversal.by(outE().id())
                    .by(outE().label());
        }
    }

    protected static void appendGetAllEdgeResultsProject(final GraphTraversal<?, ?> traversal) {
        traversal.project(MAP_KEY, IN_V_ID_KEY, IN_V_LABEL_KEY, OUT_V_ID_KEY, OUT_V_LABEL_KEY, ID_KEY)
                .by(valueMap().with(WithOptions.tokens))
                .by(inV().id())
                .by(inV().label())
                .by(outV().id())
                .by(outV().label())
                .by(id());
    }

    protected static List<Object> edgeResultToList(final List<Map<String, Object>> results,
                                                   final List<String> fieldNames,
                                                   final TableDef table) {
        final List<Object> rows = new ArrayList<>();
        for (final Map<String, Object> map : results) {
            rows.add(getEdgeRow(map, fieldNames, table));
        }
        return rows;
    }

    protected static List<Object> vertexResultsToList(final List<Map<String, Object>> results,
                                                      final List<String> fieldNames, final TableDef table) {
        final List<Object> rows = new ArrayList<>();
        for (final Map<String, Object> map : results) {
            rows.add(getVertexRow(map, fieldNames, table));
        }
        return rows;
    }

    private static Object[] getVertexRow(final Map<String, Object> map, final List<String> fieldNames,
                                         final TableDef table) {
        final Object[] row = new Object[fieldNames.size()];
        final Map<Object, Object> mapResult = (Map<Object, Object>) map.get(MAP_KEY);
        final String inEId = (String) map.get(IN_E_ID_KEY);
        final String inELabel = (String) map.get(IN_E_LABEL_KEY);
        final String outEId = (String) map.get(OUT_E_ID_KEY);
        final String outELabel = (String) map.get(OUT_E_LABEL_KEY);
        final String id = (String) map.get(ID_KEY);
        int idx = 0;
        for (final String field : fieldNames) {
            final String propName = TableUtil.getProperty(table, field);
            if (propName.toUpperCase().endsWith("_ID")) {
                final String labelName = propName.toUpperCase().replace("_ID", "");
                if (table.hasIn && inELabel != null && labelName.toUpperCase().equals(inELabel.toUpperCase())) {
                    row[idx] = inEId;
                } else if (table.hasOut && outELabel != null &&
                        labelName.toUpperCase().equals(outELabel.toUpperCase())) {
                    row[idx] = outEId;
                } else if (labelName.toUpperCase().equals(table.label.toUpperCase())) {
                    row[idx] = id;
                } else {
                    row[idx] = null;
                }
            } else {
                row[idx] = mapResult.getOrDefault(propName, null);
                if (row[idx] instanceof List) {
                    row[idx] = ((List<?>) row[idx]).get(0);
                }
                row[idx] = TableUtil.convertType(row[idx], table.getColumn(field));
            }
            idx++;
        }
        return row;
    }

    private static Object[] getEdgeRow(final Map<String, Object> map, final List<String> fieldNames,
                                       final TableDef table) {
        final Map<Object, Object> mapResult = (Map<Object, Object>) map.get(MAP_KEY);
        final String inVId = (String) map.get(IN_V_ID_KEY);
        final String inVLabel = (String) map.get(IN_V_LABEL_KEY);
        final String outVId = (String) map.get(OUT_V_ID_KEY);
        final String outVLabel = (String) map.get(OUT_V_LABEL_KEY);
        final String id = (String) map.get(ID_KEY);
        int idx = 0;
        final Object[] row = new Object[fieldNames.size()];
        for (final String field : fieldNames) {
            final String propName = TableUtil.getProperty(table, field);
            if (propName.toUpperCase().endsWith("_ID")) {
                final String labelName = propName.toUpperCase().replace("_ID", "");
                if (inVLabel != null && labelName.toUpperCase().equals(inVLabel.toUpperCase())) {
                    row[idx] = inVId;
                } else if (outVLabel != null && labelName.toUpperCase().equals(outVLabel.toUpperCase())) {
                    row[idx] = outVId;
                } else if (labelName.toUpperCase().equals(table.label.toUpperCase())) {
                    row[idx] = id;
                } else {
                    row[idx] = null;
                }
            } else {
                row[idx] = mapResult.getOrDefault(propName, null);
                if (row[idx] instanceof List) {
                    row[idx] = ((List<?>) row[idx]).get(0);
                }
                row[idx] = TableUtil.convertType(row[idx], table.getColumn(field));
            }
            idx++;
        }
        return row;
    }

    protected List<Object[]> joinResultsToList(final List<Map<String, Object>> results,
                                               final JoinVisitor.JoinMetadata joinMetadata) throws SQLException {
        final List<Object[]> rows = new ArrayList<>();
        final List<Pair<String, String>> tableColumnList = new ArrayList<>();
        for (final SqlToGremlin.GremlinSelectInfo gremlinSelectInfo : gremlinParseInfo.getGremlinSelectInfos()) {
            if (gremlinSelectInfo.getTable().equalsIgnoreCase(joinMetadata.getLeftTable().label)) {
                tableColumnList.add(new Pair<>(joinMetadata.getLeftTable().label,
                        TableUtil.getProperty(joinMetadata.getLeftTable(), gremlinSelectInfo.getColumn())));
            } else if (gremlinSelectInfo.getTable().equalsIgnoreCase(joinMetadata.getRightTable().label)) {
                tableColumnList.add(new Pair<>(joinMetadata.getRightTable().label,
                        TableUtil.getProperty(joinMetadata.getRightTable(), gremlinSelectInfo.getColumn())));
            } else {
                throw new SQLException("Error, cannot marshal results, incorrect metadata.");
            }
        }
        for (final Map<String, Object> result : results) {
            // Because of the bidirectionality, we have to check all each time.
            final Object[] row = new Object[tableColumnList.size()];
            int i = 0;
            for (final Pair<String, String> tableColumn : tableColumnList) {
                final Object data = ((Map<String, Object>) result.getOrDefault(tableColumn.getKey(), new HashMap<>()))
                        .getOrDefault(tableColumn.getValue(), null);
                if (data instanceof List) {
                    if (((List) data).size() == 1) {
                        row[i++] = ((List) data).get(0);
                    } else {
                        row[i++] = data;
                    }
                } else {
                    row[i++] = data;
                }
            }
            rows.add(row);
        }
        return rows;
    }

    // This function allows the limit to be overwritten.
    protected void imposeLimit(final GraphTraversal<?, ?> graphTraversal, final int limit) {
        if (limit != -1) {
            graphTraversal.limit(limit);
        }
    }

    protected void imposeLimit(final GraphTraversal<?, ?> graphTraversal) {
        imposeLimit(graphTraversal, gremlinParseInfo.getLimit());
    }

    protected void appendSelectTraversal(final TableDef tableDef, final GraphTraversal<?, ?> traversal)
            throws SQLException {
        final List<String> columns = new ArrayList<>();
        for (final SqlToGremlin.GremlinSelectInfo selectInfo : gremlinParseInfo.getGremlinSelectInfos()) {
            if (selectInfo.getTable().toLowerCase().equals(tableDef.label.toLowerCase())) {
                columns.add(TableUtil.getProperty(tableDef, selectInfo.getColumn()));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("Failed to find columns for table to use in traversal.");
        }
        traversal.valueMap(columns.toArray(new String[] {}));
    }

    protected GraphTraversal<?, ?> getSelectTraversal(final TableDef tableDef) throws SQLException {
        final List<String> columns = new ArrayList<>();
        for (final SqlToGremlin.GremlinSelectInfo selectInfo : gremlinParseInfo.getGremlinSelectInfos()) {
            if (selectInfo.getTable().toLowerCase().equals(tableDef.label.toLowerCase())) {
                columns.add(TableUtil.getProperty(tableDef, selectInfo.getColumn()));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("Failed to find columns for table to use in traversal.");
        }
        return __.valueMap(columns.toArray(new String[] {}));
    }

    @Getter
    public static class SqlGremlinQueryResult {
        private final List<String> columns;
        private final List<String> columnTypes = new ArrayList<>();
        private final List<List<Object>> rows = new ArrayList<>();

        SqlGremlinQueryResult(final List<String> columns, final List<?> rows, final List<TableDef> tableConfigs) {
            this.columns = columns;
            for (final Object row : rows) {
                final List<Object> convertedRow = new ArrayList<>();
                if (row instanceof Object[]) {
                    convertedRow.addAll(Arrays.asList((Object[]) row));
                } else {
                    convertedRow.add(row);
                }
                this.rows.add(convertedRow);
            }

            for (final String column : columns) {
                TableColumn col = null;
                for (final TableDef tableConfig : tableConfigs) {
                    if (tableConfig.columns.containsKey(column)) {
                        col = tableConfig.getColumn(column);
                        break;
                    }
                }
                columnTypes.add((col == null || col.getType() == null) ? "string" : col.getType());
            }
        }
    }
}
