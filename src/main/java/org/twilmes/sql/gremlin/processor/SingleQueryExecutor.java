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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.rel.GremlinTraversalScan;
import org.twilmes.sql.gremlin.rel.GremlinTraversalToEnumerableRelConverter;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.twilmes.sql.gremlin.processor.RelUtils.isConvertable;

/**
 * Executes a query that does not have any joins.
 *
 * select * from customer where name = 'Joe'
 *
 * Created by twilmes on 12/4/15.
 */
public class SingleQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleQueryExecutor.class);
    private final RelNode node;
    private final GraphTraversal<?, ?> traversal;
    private final TableDef table;

    public SingleQueryExecutor(final RelNode node, final GraphTraversal<?, ?> traversal, final TableDef table) {
        this.node = node;
        this.traversal = traversal;
        this.table = table;
    }

    public List<Object> run() {
        LOGGER.debug("SingleQueryExecutor.run().");
        List<Object> rowResults;
        if(!isConvertable(node)) {
            // go until we hit a converter to find the input
            RelNode input = node;
            RelNode parent = node;
            while(!((input = input.getInput(0)) instanceof GremlinToEnumerableConverter)) {parent = input;};
            final RelDataType rowType = input.getRowType();

            final List<String> fieldNames = rowType.getFieldNames();

            final List<Object> results = traversal.as("table_0").select("table_0").toList();
            final List<Object> rows = new ArrayList<>();

            for(Object o : results) {
                boolean skip = false;
                Element res = (Element) o;
                Object[] row = new Object[fieldNames.size()];
                int colNum = 0;
                for(String field : fieldNames) {
                    final String propName = TableUtil.getProperty(table, field);
                    int keyIndex = propName.toLowerCase().indexOf("_id");
                    Object val = null;
                    if(keyIndex > 0) {
                        // is it a pk or fk?
                        final String key = propName.substring(0, keyIndex);
                        if(table.label.toLowerCase().equals(key.toLowerCase())) {
                            val = res.id();
                        } else {
                            // todo add fk (connected vertex) ids
                        }
                    } else if (!(res.property(propName) instanceof EmptyProperty)) {
                        if (skip = !res.keys().contains(propName)) {
                            // TODO: Fix this skip stuff
                            val = "foo";
                        } else {
                            val = res.property(propName).value();
                            val = TableUtil.convertType(val, table.getColumn(field));
                        }
                    }
                    row[colNum] = val;
                    colNum++;
                }
                rows.add(row);
            }

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
            final List<Map<Object, Object>> results = traversal.valueMap().toList();
            final List<Object> rows = new ArrayList<>();
            final List<String> fieldNames = node.getRowType().getFieldNames();
            for(Map<Object, Object> res : results) {
                Object[] row = new Object[fieldNames.size()];
                int colNum = 0;
                for(String field : fieldNames) {
                    String propName = TableUtil.getProperty(table, field);
                    if(res.containsKey(propName)) {
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
}
