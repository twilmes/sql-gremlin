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
    // default to 1000
    private static int pageSize = DEFAULT_PAGE_SIZE;
    // calcite relational expression
    private final RelNode node;
    // defines table format
    private final TableDef table;
    // gremlin traversal
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

            // go through each result
            for (final Object o : results) {
                // gremlin result object
                final Element res = (Element) o;
                // a list of objects
                final Object[] row = new Object[fieldNames.size()];
                int colNum = 0;
                // for all the fields
                for (final String field : fieldNames) {
                    // get property name
                    final String propName = TableUtil.getProperty(table, field);
                    final int keyIndex = propName.toLowerCase().indexOf("_id");
                    // get value
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
                    // put value into row
                    row[colNum] = val;
                    colNum++;
                }
                // add row into result row list
                rows.add(row);
            }

            // create a traversal scan object
            final GremlinTraversalScan traversalScan =
                    new GremlinTraversalScan(input.getCluster(), input.getTraitSet(), rowType, rows);

            // converts traversal to rel
            final GremlinTraversalToEnumerableRelConverter converter =
                    new GremlinTraversalToEnumerableRelConverter(input.getCluster(),
                            input.getTraitSet(), traversalScan, rowType);
            parent.replaceInput(0, converter);
            final Bindable bindable = EnumerableInterpretable.toBindable(ImmutableMap.of(), null,
                    (EnumerableRel) node, EnumerableRel.Prefer.ARRAY);
            final Enumerable<Object> enumerable = bindable.bind(null);
            rowResults = enumerable.toList();
        } else {
            // if it is a convertable node, traversal gets the values
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
            // use the traversal to get results
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

        // go until we hit a converter to find the input
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

        //        try {
        //            executor.awaitTermination(20, TimeUnit.SECONDS);
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }
        return sqlGremlinQueryResult;
        //        return convertResult(rows, input, parent, rowType);
    }

    public SqlGremlinQueryResult handleEdge() {
        if (isConvertable(node)) {
            return null;
        }

        // Go until we hit a converter to find the input.
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
        //        try {
        //            executor.awaitTermination(20, TimeUnit.SECONDS);
        //        } catch (InterruptedException e) {
        //            e.printStackTrace();
        //        }

        return sqlGremlinQueryResult;
        //return convertResult(rows, input, parent, rowType);
    }

    //    SqlGremlinQueryResult convertResult(final List<Object> rows, final RelNode input, final RelNode parent,
    //                                        final RelDataType rowType) {
    //        return new SqlGremlinQueryResult(input.getCluster().getPlanner().getRoot().getRowType().getFieldNames(), table);
    //    }

    // converts input row results and insert them into sqlGremlinQueryResult
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
        // private final List<List<Object>> rows = new ArrayList<>();
        // blocking queue is shared between threads, here we use it so we can take elements out of it, while we put
        // elements inside

        // blocking queue is used when you have multiple threads put and take items from it, so do we want to use the queue
        // in the way that set the queue size to page limit, then have launch a thread that continuously gets all results
        // dumping them into the queue, then a full queue will block the thread from adding elements, then the results
        // get dumped into a list to be returned? At that point the thread will continue to dump result

        // or do we want an unbounded blocking queue, for which we will only block on empty, and then continuously put
        // results into the queue, but have the thread monitor size of the result list and insert when we are not at capacity?
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
                // insert specific stop signal
                // blockingQueueRows.add(null);
                // interrupt here?
                // Thread.currentThread().interrupt();
                //
                if (currThread != null && blockingQueueRows.size() == 0) {
                    System.out.println("assertIsEmpty() INTERRUPT THREAD CALL");
                    currThread.interrupt();
                }
                isEmpty = true;
            }
        }

        // add result here needs to be the converted rows
        public void addResults(final List<List<Object>> rows) {
            blockingQueueRows.addAll(rows);
        }

        // get result blocks if the queue is empty, so we have to make sure that is empty doesn't change when it is
        // waiting for the queue;
        // is it possible for us to issue getResult --> result set not empty yet but empty queue --> starts waiting
        // --> then the thread asserts it is empty --> but we are stuck waiting?
        public Object getResult() {
            try {
                synchronized (assertEmptyLock) {
                    System.out.println("getResult() INSIDE LOCK");
                    // pass current thread in, and interrupt in assertIsEmpty
                    this.currThread = Thread.currentThread();
                    if (isEmpty && blockingQueueRows.size() == 0) {
                        System.out.println("getResult() IS EMPTY");
                        return null;
                    }
                }
                System.out.println("getResult() CURRENT SIZE: " + this.blockingQueueRows.size());
                System.out.println("getResult() RETURN RESULT");
                return this.blockingQueueRows.take();
            } catch (final InterruptedException ignored) {
                System.out.println("getResult() INTERRUPTED: " + ignored);
                return null;
            }
        }

        public int getPageSize() {
            return pageSize;
        }

        // TODO: should this be something SqlGremlinQueryResult manage? this is done only because SqlGremlinQueryResult
        //  is the only object being passed through the ResultSet constructor
        public void setPageSize(int size) {
            if (size <= 0) {
                pageSize = DEFAULT_PAGE_SIZE;
            }
            pageSize = size;
        }
    }

    public class VertexPagination implements Runnable {
        private RelNode input;
        private RelNode parent;
        private int cumulativeRes = 0;

        VertexPagination(final RelNode input, final RelNode parent) {
            this.input = input;
            this.parent = parent;
        }

        @Override
        public void run() {
            System.out.println("VERTEX PAGINATION START");
            // Monitor size of sqlGremlinQueryResult list and insert on the fly.
            while (traversal.hasNext()) {
                System.out.println("VERTEX PAGINATION BATCH START SIZE: " + cumulativeRes);
                final RelDataType rowType = input.getRowType();
                final List<String> fieldNames = rowType.getFieldNames();
                final List<Map<String, Object>> results = (List<Map<String, Object>>) traversal.next(pageSize);
                cumulativeRes += results.size();
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
                System.out.println("VERTEX PAGINATION BATCH DONE");
                convertAndInsertResult(sqlGremlinQueryResult, rows, input, parent, rowType);
            }
            // If we run out of traversal data (or hit our limit), stop inserting and signal to the result that it is done.
            System.out.println("VERTEX PAGINATION DONE. FINAL SIZE: " + cumulativeRes);
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
            System.out.println("EDGE PAGINATION START");
            // Monitor size of sqlGremlinQueryResult list and insert on the fly.
            while (traversal.hasNext()) {
                System.out.println("EDGE PAGINATION BATCH START");
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
                System.out.println("EDGE PAGINATION BATCH DONE");
                convertAndInsertResult(sqlGremlinQueryResult, rows, input, parent, rowType);
            }
            // If we run out of traversal data (or hit our limit), stop inserting and signal to the result that it is done.
            System.out.println("EDGE PAGINATION DONE");
            sqlGremlinQueryResult.assertIsEmpty();
        }
    }
}
