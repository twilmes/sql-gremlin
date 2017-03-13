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

package org.twilmes.sql.gremlin.processor;

import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.rel.GremlinTraversalScan;
import org.twilmes.sql.gremlin.rel.GremlinTraversalToEnumerableRelConverter;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyVertexProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.twilmes.sql.gremlin.processor.RelUtils.isConvertable;

/**
 * Executes queries that contain 1 or more joins.
 *
 * Created by twilmes on 12/4/15.
 */
public class JoinQueryExecutor {

    private final RelNode node;
    private final Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap;
    private final GraphTraversal<?, ?> traversal;
    private final Map<GremlinToEnumerableConverter, String> tableIdMap;

    public JoinQueryExecutor(final RelNode node,
                             final Map<EnumerableJoin, Map<String, GremlinToEnumerableConverter>> fieldMap,
                             GraphTraversal<?, ?> traversal, Map<GremlinToEnumerableConverter, String> tableIdMap) {
        this.node = node;
        this.fieldMap = fieldMap;
        this.traversal = traversal;
        this.tableIdMap = tableIdMap;
    }

    public List<Object> run() {
        List<Object> rowResults;
        if(!isConvertable(node)) {
            // go until we hit a converter to find the input
            RelNode input = node;
            RelNode parent = node;
            while(!((input = input.getInput(0)) instanceof EnumerableJoin)) {parent = input;};
            final RelDataType rowType = input.getRowType();

            final List<String> fieldNames = rowType.getFieldNames();
            final List<String> tableIds = new ArrayList(tableIdMap.values());
            if(tableIds.size() == 1) {
                traversal.select(tableIds.get(0));
            } else if(tableIds.size() == 2) {
                traversal.select(tableIds.get(0), tableIds.get(1));
            } else {
                String[] remainingIds = tableIds.subList(2, tableIds.size()).toArray(new String[tableIds.size() - 2]);
                traversal.select(tableIds.get(0), tableIds.get(1), remainingIds);
            }

            final List<Map<String, ? extends Element>> results = (List<Map<String, ? extends Element>>) traversal.toList();

            final EnumerableJoin join = (EnumerableJoin) input;
            // transform to proper project order
            final Map<String, String> fieldToTableMap = new HashMap<>();
            final Map<String, TableDef> tableIdToTableDefMap = new HashMap<>();
            for(Map.Entry<String, GremlinToEnumerableConverter> entry : fieldMap.get(join).entrySet()) {
                // for each field, map back to table id
                final String tableId = tableIdMap.get(entry.getValue());
                fieldToTableMap.put(entry.getKey(), tableId);
                tableIdToTableDefMap.put(tableId, TableUtil.getTableDef(entry.getValue()));
            }

            // generate project function
            final List<Object> rows = project(fieldToTableMap, fieldNames, results, tableIdToTableDefMap);

            final GremlinTraversalScan traversalScan =
                    new GremlinTraversalScan(input.getCluster(), input.getTraitSet(),
                            rowType, rows);

            final GremlinTraversalToEnumerableRelConverter converter = new GremlinTraversalToEnumerableRelConverter(input.getCluster(),
                    input.getTraitSet(), traversalScan, rowType);

            parent.replaceInput(0, converter);

            final Bindable bindable = EnumerableInterpretable.toBindable(null, null,
                    (EnumerableRel) node, EnumerableRel.Prefer.ARRAY);

            final Enumerable<Object> enumerable = bindable.bind(null);

            rowResults = enumerable.toList();
        } else {
            // we want everything so we don't need to convert this into a GremlinTraversalScan and then feed
            // it into the Calcite operator tree
            final RelNode input = node;
            final RelDataType rowType = input.getRowType();

            final List<String> fieldNames = rowType.getFieldNames();
            final List<String> tableIds = new ArrayList(tableIdMap.values());
            if(tableIds.size() == 1) {
                traversal.select(tableIds.get(0));
            } else if(tableIds.size() == 2) {
                traversal.select(tableIds.get(0), tableIds.get(1));
            } else {
                String[] remainingIds = tableIds.subList(2, tableIds.size()).toArray(new String[tableIds.size() - 2]);
                traversal.select(tableIds.get(0), tableIds.get(1), remainingIds);
            }

            final List<Map<String, ? extends Element>> results = (List<Map<String, ? extends Element>>) traversal.toList();

            final EnumerableJoin join = (EnumerableJoin) input;
            // transform to proper project order
            final Map<String, String> fieldToTableMap = new HashMap<>();
            final Map<String, TableDef> tableIdToTableDefMap = new HashMap<>();
            for(Map.Entry<String, GremlinToEnumerableConverter> entry : fieldMap.get(join).entrySet()) {
                // for each field, map back to table id
                final String tableId = tableIdMap.get(entry.getValue());
                fieldToTableMap.put(entry.getKey(), tableId);
                tableIdToTableDefMap.put(tableId, TableUtil.getTableDef(entry.getValue()));
            }

            // generate project function
            rowResults = project(fieldToTableMap, fieldNames, results, tableIdToTableDefMap);
        }
        return rowResults;
    }

    private List<Object> project(Map<String, String> fieldToTableMap, List<String> fields,
                                 List<Map<String, ? extends Element>> results,
                                 Map<String, TableDef> tableIdToTableDefMap) {
        final List<Object> rows = new ArrayList<>(results.size());
        Map<String, String> labelTableIdMap = new HashMap<>();
        for (Map.Entry<String, TableDef> entry : tableIdToTableDefMap.entrySet()) {
            labelTableIdMap.put(entry.getValue().label.toLowerCase(), entry.getKey());
        }
        for(Map<String, ? extends Element> result : results) {
            final Object[] row = new Object[fields.size()];
            int column = 0;
            for(String field : fields) {
                String tableId = fieldToTableMap.get(field);
                String simpleFieldName = Character.isDigit(field.charAt(field.length()-1)) ?
                        field.substring(0, field.length()-1) : field;
                simpleFieldName = Character.isDigit(field.charAt(simpleFieldName.length()-1)) ?
                        simpleFieldName.substring(0, simpleFieldName.length()-1) : simpleFieldName;
                // check for primary & fks
                final int keyIndex = simpleFieldName.toLowerCase().indexOf("_id");
                Object val = null;
                if(keyIndex > 0) {
                    // is it a pk or fk?
                    String key = simpleFieldName.substring(0, keyIndex);
                    String tableLabel = tableIdToTableDefMap.get(tableId).label;
                    if(tableLabel.toLowerCase().equals(key.toLowerCase())) {
                        val = result.get(tableId).id();
                    } else {
                        String fkTableId = labelTableIdMap.get(key.toLowerCase());
                        if(result.containsKey(fkTableId)) {
                            val = result.get(fkTableId).id();
                        }
                    }
                }
                TableColumn tableColumn = tableIdToTableDefMap.get(tableId).getColumn(simpleFieldName.toLowerCase());
                final Property<Object> property = tableColumn == null ? Property.empty() : result.get(tableId).property(tableColumn.getPropertyName());
                if(!(property instanceof EmptyProperty || property instanceof EmptyVertexProperty)) {
                    if(result.get(tableId).label().equals(tableIdToTableDefMap.get(tableId).label)) {
                        val = property.value();
                    } else {
                        val = null;
                    }
                }
                if(tableIdToTableDefMap.get(tableId).getColumn(field) != null && val != null) {
                    row[column++] = TableUtil.convertType(val, tableIdToTableDefMap.get(tableId).getColumn(field));
                } else {
                    row[column++] = val;
                }
            }
            rows.add(row);
        }
        return rows;
    }

}
