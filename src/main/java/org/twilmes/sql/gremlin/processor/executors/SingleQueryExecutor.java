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
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.rel.GremlinTraversalScan;
import org.twilmes.sql.gremlin.rel.GremlinTraversalToEnumerableRelConverter;
import org.twilmes.sql.gremlin.schema.TableDef;
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
public class SingleQueryExecutor extends QueryExecutor {
    private final RelNode node;
    private final GraphTraversal<?, ?> traversal;
    private final TableDef table;

    public SingleQueryExecutor(final RelNode node, final GraphTraversal<?, ?> traversal, final TableDef table,
                               final SqlToGremlin.GremlinParseInfo gremlinParseInfo) {
        super(gremlinParseInfo);
        this.node = node;
        this.traversal = traversal;
        this.table = table;
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
        appendGetAllVertexResultsProject(table, traversal);
        final List<Map<String, Object>> results = (List<Map<String, Object>>) traversal.limit(10000).toList();
        final List<Object> rows = vertexResultsToList(results, fieldNames, table);
        return convertResult(rows, input, parent, rowType);
    }

    public SqlGremlinQueryResult handleEdge() {
        if (isConvertable(node)) {
            return null;
        }

        // Go until we hit a converter to find the input.
        RelNode input = node;
        RelNode parent = node;
        while (!((input = input.getInput(0)) instanceof GremlinToEnumerableConverter)) {
            parent = input;
        }
        final RelDataType rowType = input.getRowType();
        final List<String> fieldNames = rowType.getFieldNames();
        appendGetAllEdgeResultsProject(traversal);
        final List<Map<String, Object>> results =
                (List<Map<String, Object>>) traversal.limit(10000).toList();
        final List<Object> rows = edgeResultToList(results, fieldNames, table);
        return convertResult(rows, input, parent, rowType);
    }

    SqlGremlinQueryResult convertResult(final List<Object> rows, final RelNode input, final RelNode parent,
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
        return new SqlGremlinQueryResult(input.getCluster().getPlanner().getRoot().getRowType().getFieldNames(),
                rowResults, ImmutableList.of(table));
    }

}
