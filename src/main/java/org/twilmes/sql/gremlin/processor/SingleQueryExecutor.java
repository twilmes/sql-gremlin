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

import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.rel.GremlinTraversalScan;
import org.twilmes.sql.gremlin.rel.GremlinTraversalToEnumerableRelConverter;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.twilmes.sql.gremlin.processor.RelUtils.isConvertable;

/**
 * Executes a query that does not have any joins.
 * <p>
 * select * from customer where name = 'Joe'
 * <p>
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class SingleQueryExecutor {
    private final RelNode node;
    private final GraphTraversal<?, ?> traversal;
    private final TableDef table;

    public SingleQueryExecutor(final RelNode node, final GraphTraversal<?, ?> traversal, final TableDef table) {
        this.node = node;
        this.traversal = traversal;
        this.table = table;
    }

    public List<Object> run() {
        final List<Object> rowResults;
        if (!isConvertable(node)) {
            // go until we hit a converter to find the input
            RelNode input = node;
            RelNode parent = node;
            while (!((input = input.getInput(0)) instanceof GremlinToEnumerableConverter)) {
                parent = input;
            }
            final RelDataType rowType = input.getRowType();

            final List<String> fieldNames = rowType.getFieldNames();
            final List<Object> results = traversal.as("table_0").select("table_0").toList();
            final List<Object> rows = new ArrayList<>();


            for (final Object o : results) {
                final Element res = (Element) o;
                final Object[] row = new Object[fieldNames.size()];
                int colNum = 0;
                for (final String field : fieldNames) {
                    final String propName = TableUtil.getProperty(table, field);
                    final int keyIndex = propName.toLowerCase().indexOf("_id");
                    Object val = null;
                    if (keyIndex > 0) {
                        // is it a pk or fk?
                        final String key = propName.substring(0, keyIndex);
                        if (table.label.toLowerCase().equals(key.toLowerCase())) {
                            val = res.id();
                        } else {
                            // todo add fk (connected vertex) ids
                        }
                    } else if (!(res.property(propName) instanceof EmptyProperty)) {
                        val = res.property(propName).value();
                        val = TableUtil.convertType(val, table.getColumn(field));
                    }
                    row[colNum] = val;
                    colNum++;
                }
                rows.add(row);
            }

            final GremlinTraversalScan traversalScan =
                    new GremlinTraversalScan(input.getCluster(), input.getTraitSet(),
                            rowType, rows);

            final GremlinTraversalToEnumerableRelConverter converter =
                    new GremlinTraversalToEnumerableRelConverter(input.getCluster(),
                            input.getTraitSet(), traversalScan, rowType);
            parent.replaceInput(0, converter);
            final Bindable bindable = EnumerableInterpretable.toBindable(null, null,
                    (EnumerableRel) node, EnumerableRel.Prefer.ARRAY);
            final Enumerable<Object> enumerable = bindable.bind(null);
            rowResults = enumerable.toList();
        } else {
            final List<Map<Object, Object>> results = traversal.valueMap().toList();
            final List<Object> rows = new ArrayList<>();
            final List<String> fieldNames = node.getRowType().getFieldNames();
            for (final Map<Object, Object> res : results) {
                final Object[] row = new Object[fieldNames.size()];
                int colNum = 0;
                for (final String field : fieldNames) {
                    final String propName = TableUtil.getProperty(table, field);
                    if (res.containsKey(propName)) {
                        Object val = ((List) res.get(propName)).get(0);
                        val = TableUtil.convertType(val, table.getColumn(field));
                        row[colNum] = val;
                    }
                    colNum++;
                }
                rows.add(row);
            }
            rowResults = rows;
        }
        return rowResults;
    }

    public SqlGremlinQueryResult handle() {
        SqlGremlinQueryResult result = null;
        if (!isConvertable(node)) {
            // go until we hit a converter to find the input
            RelNode input = node;
            RelNode parent = node;
            while (!((input = input.getInput(0)) instanceof GremlinToEnumerableConverter)) {
                parent = input;
            }
            final RelDataType rowType = input.getRowType();

            final List<String> fieldNames = rowType.getFieldNames();

            final List<Map<Object, Object>> results = traversal.valueMap().with(WithOptions.tokens).toList();
            final List<Object> rows = new ArrayList<>();
            int idx = 0;
            for (final Map<Object, Object> mapResult : results) {
                idx = 0;
                final Object[] row = new Object[fieldNames.size()];
                for (final String field : fieldNames) {
                    // TODO: Use propName instead of field after this, it handles looking it table for case sensitive version.
                    final String propName = TableUtil.getProperty(table, field);
                    final int keyIndex = propName.toLowerCase().indexOf("_id");
                    Object val = null;
                    if (keyIndex > 0) {
                        // Could be PK or FK.
                        final String key = propName.substring(0, keyIndex);
                        if (table.label.toLowerCase().equals(key.toLowerCase())) {
                            val = mapResult.get(field);
                        } else {
                            // todo add fk (connected vertex) ids
                        }
                    } else {
                        if (mapResult.containsKey(propName)) {
                            val = ((List) mapResult.get(field.toLowerCase())).get(0);
                            val = TableUtil.convertType(val, table.getColumn(field));
                        }
                    }
                    row[idx] = val;
                    idx++;
                }
                rows.add(row);
            }

            final GremlinTraversalScan traversalScan =
                    new GremlinTraversalScan(input.getCluster(), input.getTraitSet(),
                            rowType, rows);

            final GremlinTraversalToEnumerableRelConverter converter =
                    new GremlinTraversalToEnumerableRelConverter(input.getCluster(),
                            input.getTraitSet(), traversalScan, rowType);
            parent.replaceInput(0, converter);
            final Bindable bindable = EnumerableInterpretable.toBindable(null, null,
                    (EnumerableRel) node, EnumerableRel.Prefer.ARRAY);
            final Enumerable<Object> enumerable = bindable.bind(null);
            final List<Object> rowResults = enumerable.toList();
            result = new SqlGremlinQueryResult(node.getRowType().getFieldNames(), rowResults);
        }
        return result;
    }

    public class SqlGremlinQueryResult {
        List<String> columns;
        List<Object> rows;

        SqlGremlinQueryResult(final List<String> columns, final List<Object> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<Object> getRows() {
            return rows;
        }
    }
}
