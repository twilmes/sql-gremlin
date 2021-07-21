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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
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
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.id;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
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
    private static final String IN_V_ID_KEY = "__in_v_id__";
    private static final String OUT_V_ID_KEY = "__out_v_id__";
    private static final String IN_V_LABEL_KEY = "__in_v_label__";
    private static final String OUT_V_LABEL_KEY = "__out_v_label__";
    private static final String IN_E_ID_KEY = "__in_e_id__";
    private static final String OUT_E_ID_KEY = "__out_e_id__";
    private static final String IN_E_LABEL_KEY = "__in_e_label__";
    private static final String OUT_E_LABEL_KEY = "__out_e_label__";
    private static final String MAP_KEY = "__map__";
    private static final String ID_KEY = "__id__";
    private static final long RESULT_LIMIT = 10000;
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    private final RelNode node;
    private final TableDef table;
    private GraphTraversal<?, ?> traversal;
    private SqlGremlinQueryResult sqlGremlinQueryResult;

    public SingleQueryExecutor(final RelNode node, final GraphTraversal<?, ?> traversal, final TableDef table) {
        this.node = node;
        this.traversal = traversal;
        this.table = table;
    }

    public List<Object> run() {
        final List<Object> rowResults;
        if (!isConvertable(node)) {
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
                    new GremlinTraversalScan(input.getCluster(), input.getTraitSet(), rowType, rows);

            final GremlinTraversalToEnumerableRelConverter converter =
                    new GremlinTraversalToEnumerableRelConverter(input.getCluster(),
                            input.getTraitSet(), traversalScan, rowType);
            parent.replaceInput(0, converter);
            final Bindable bindable = EnumerableInterpretable.toBindable(ImmutableMap.of(), null,
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

    // TODO: At some point the function below should be consolidated.
    // This is a partially consolidated traversal executor.
    List<Map<String, Object>> executeTraversal(final boolean isVertex) {
        final List<String> additionalKeys = new ArrayList<>();
        additionalKeys.add(ID_KEY);
        if (isVertex) {
            if (table.hasIn) {
                additionalKeys.add(IN_E_ID_KEY);
                additionalKeys.add(IN_E_LABEL_KEY);
            }
            if (table.hasOut) {
                additionalKeys.add(OUT_E_ID_KEY);
                additionalKeys.add(OUT_E_LABEL_KEY);
            }
        } else {
            additionalKeys.add(IN_V_ID_KEY);
            additionalKeys.add(IN_V_LABEL_KEY);
            additionalKeys.add(OUT_V_ID_KEY);
            additionalKeys.add(OUT_V_LABEL_KEY);
        }
        traversal.project(MAP_KEY, additionalKeys.toArray(new String[0]))
                .by(valueMap().with(WithOptions.tokens))
                .by(id());
        if (isVertex) {
            if (table.hasIn) {
                traversal.by(inE().outV().id())
                        .by(inE().outV().label());
            }
            if (table.hasOut) {
                traversal.by(outE().inV().id())
                        .by(outE().inV().label());
            }
        } else {
            traversal.by(inV().id())
                    .by(inV().label())
                    .by(outV().id())
                    .by(outV().label());
        }
        return (List<Map<String, Object>>) traversal.limit(RESULT_LIMIT).toList();
    }

    public SqlGremlinQueryResult handleVertex() {
        if (isConvertable(node)) {
            return null;
        }

        RelNode input = node;
        RelNode parent = node;
        while (!((input = input.getInput(0)) instanceof GremlinToEnumerableConverter)) {
            parent = input;
        }
        final RelDataType rowType = input.getRowType();
        final List<String> fieldNames = rowType.getFieldNames();

        final List<String> additionalKeys = new ArrayList<>();
        additionalKeys.add(ID_KEY);
        if (table.hasIn) {
            additionalKeys.add(IN_E_ID_KEY);
            additionalKeys.add(IN_E_LABEL_KEY);
        }
        if (table.hasOut) {
            additionalKeys.add(OUT_E_ID_KEY);
            additionalKeys.add(OUT_E_LABEL_KEY);
        }

        traversal.project(MAP_KEY, additionalKeys.toArray(new String[0]))
                .by(valueMap().with(WithOptions.tokens))
                .by(id());
        if (table.hasIn) {
            traversal.by(inE().outV().id())
                    .by(inE().outV().label());
        }
        if (table.hasOut) {
            traversal.by(outE().inV().id())
                    .by(outE().inV().label());
        }

        sqlGremlinQueryResult =
                new SqlGremlinQueryResult(input.getCluster().getPlanner().getRoot().getRowType().getFieldNames(),
                        table);

        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread").setDaemon(true).build());

        final Runnable pagination = new VertexPagination(input, parent);
        executor.execute(pagination);
        executor.shutdown();

        return sqlGremlinQueryResult;
    }

    public SqlGremlinQueryResult handleEdge() {
        if (isConvertable(node)) {
            return null;
        }

        RelNode input = node;
        RelNode parent = node;
        while (!((input = input.getInput(0)) instanceof GremlinToEnumerableConverter)) {
            // TODO: Figure out how to push this into the traversal.
            if (input instanceof EnumerableLimit) {
                System.out.println("EnumerableLimit");
            }
            parent = input;
        }
        final RelDataType rowType = input.getRowType();
        final List<String> fieldNames = rowType.getFieldNames();

        traversal = traversal.project(MAP_KEY, IN_V_ID_KEY, IN_V_LABEL_KEY, OUT_V_ID_KEY, OUT_V_LABEL_KEY, ID_KEY)
                .by(valueMap().with(WithOptions.tokens))
                .by(inV().id())
                .by(inV().label())
                .by(outV().id())
                .by(outV().label())
                .by(id()).limit(RESULT_LIMIT);

        sqlGremlinQueryResult =
                new SqlGremlinQueryResult(input.getCluster().getPlanner().getRoot().getRowType().getFieldNames(),
                        table);

        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread").setDaemon(true).build());

        final Runnable pagination = new EdgePagination(input, parent);
        executor.execute(pagination);
        executor.shutdown();

        return sqlGremlinQueryResult;
    }

    /**
     * converts input row results and insert them into sqlGremlinQueryResult
     */
    void convertAndInsertResult(final SqlGremlinQueryResult sqlGremlinQueryResult, final List<Object> rows,
                                final RelNode input, final RelNode parent,
                                final RelDataType rowType) {
        final GremlinTraversalScan traversalScan =
                new GremlinTraversalScan(input.getCluster(), input.getTraitSet(), rowType, rows);

        final GremlinTraversalToEnumerableRelConverter converter =
                new GremlinTraversalToEnumerableRelConverter(input.getCluster(), input.getTraitSet(), traversalScan,
                        rowType);
        parent.replaceInput(0, converter);
        final Bindable<?> bindable =
                EnumerableInterpretable
                        .toBindable(ImmutableMap.of(), null, (EnumerableRel) node, EnumerableRel.Prefer.ARRAY);

        final Enumerable<?> enumerable = bindable.bind(null);
        final List<?> rowResults = enumerable.toList();

        final List<List<Object>> finalRowResult = new ArrayList<>();
        for (final Object row : rowResults) {
            final List<Object> convertedRow = new ArrayList<>();
            if (row instanceof Object[]) {
                convertedRow.addAll(Arrays.asList((Object[]) row));
            } else {
                convertedRow.add(row);
            }
            finalRowResult.add(convertedRow);
        }
        sqlGremlinQueryResult.addResults(finalRowResult);
    }

    @Getter
    public static class SqlGremlinQueryResult {
        private final List<String> columns;
        private final List<String> columnTypes = new ArrayList<>();
        private final Object assertEmptyLock = new Object();
        private final BlockingQueue<List<Object>> blockingQueueRows = new LinkedBlockingQueue<>();
        private boolean isEmpty = false;
        private Thread currThread = null;

        SqlGremlinQueryResult(final List<String> columns, final TableDef tableConfigs) {
            this.columns = columns;

            for (final String column : columns) {
                TableColumn col = null;
                if (tableConfigs.columns.containsKey(column)) {
                    col = tableConfigs.getColumn(column);
                }
                columnTypes.add((col == null || col.getType() == null) ? "string" : col.getType());
            }
        }

        public void assertIsEmpty() {
            synchronized (assertEmptyLock) {
                if (currThread != null && blockingQueueRows.size() == 0) {
                    currThread.interrupt();
                }
                isEmpty = true;
            }
        }

        public void addResults(final List<List<Object>> rows) {
            blockingQueueRows.addAll(rows);
        }

        public Object getResult() {
            try {
                synchronized (assertEmptyLock) {
                    // pass current thread in, and interrupt in assertIsEmpty
                    this.currThread = Thread.currentThread();
                    if (isEmpty && blockingQueueRows.size() == 0) {
                        return null;
                    }
                }
                return this.blockingQueueRows.take();
            } catch (final InterruptedException ignored) {
                return null;
            }
        }
    }

    public class VertexPagination implements Runnable {
        private RelNode input;
        private RelNode parent;

        VertexPagination(final RelNode input, final RelNode parent) {
            this.input = input;
            this.parent = parent;
        }

        @Override
        public void run() {
            while (traversal.hasNext()) {
                final RelDataType rowType = input.getRowType();
                final List<String> fieldNames = rowType.getFieldNames();
                final List<Map<String, Object>> results = (List<Map<String, Object>>) traversal.next(pageSize);
                final List<Object> rows = new ArrayList<>();
                for (final Map<String, Object> map : results) {
                    final Map<Object, Object> mapResult = (Map<Object, Object>) map.get(MAP_KEY);
                    final String inEId = (String) map.get(IN_E_ID_KEY);
                    final String inELabel = (String) map.get(IN_E_LABEL_KEY);
                    final String outEId = (String) map.get(OUT_E_ID_KEY);
                    final String outELabel = (String) map.get(OUT_E_LABEL_KEY);
                    final String id = (String) map.get(ID_KEY);
                    int idx = 0;
                    final Object[] row = new Object[fieldNames.size()];
                    for (final String field : fieldNames) {
                        final String propName = TableUtil.getProperty(table, field);
                        if (propName.toUpperCase().endsWith("_ID")) {
                            final String labelName = propName.toUpperCase().replace("_ID", "");
                            if (table.hasIn && inELabel != null &&
                                    labelName.toUpperCase().equals(inELabel.toUpperCase())) {
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
                    rows.add(row);
                }
                convertAndInsertResult(sqlGremlinQueryResult, rows, input, parent, rowType);
            }
            // If we run out of traversal data (or hit our limit), stop and signal to the result that it is done.
            sqlGremlinQueryResult.assertIsEmpty();
        }
    }

    public class EdgePagination implements Runnable {
        private final RelNode input;
        private final RelNode parent;

        EdgePagination(final RelNode input, final RelNode parent) {
            this.input = input;
            this.parent = parent;
        }

        @Override
        public void run() {
            while (traversal.hasNext()) {
                final RelDataType rowType = input.getRowType();
                final List<String> fieldNames = rowType.getFieldNames();

                final List<Map<String, Object>> results = (List<Map<String, Object>>) traversal.next(pageSize);

                final List<Object> rows = new ArrayList<>();
                for (final Map<String, Object> map : results) {
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
                    rows.add(row);
                }
                convertAndInsertResult(sqlGremlinQueryResult, rows, input, parent, rowType);
            }
            // If we run out of traversal data (or hit our limit), stop and signal to the result that it is done.
            sqlGremlinQueryResult.assertIsEmpty();
        }
    }
}
