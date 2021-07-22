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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.twilmes.sql.gremlin.SqlToGremlin;
import org.twilmes.sql.gremlin.processor.visitors.JoinVisitor;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Executes queries that contain 1 or more joins.
 * <p>
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class JoinQueryExecutor extends QueryExecutor {
    private final SchemaConfig schemaConfig;

    public JoinQueryExecutor(final SchemaConfig schemaConfig, final SqlToGremlin.GremlinParseInfo gremlinParseInfo) {
        super(gremlinParseInfo);
        this.schemaConfig = schemaConfig;
    }

    public SingleQueryExecutor.SqlGremlinQueryResult handle(final JoinVisitor.JoinMetadata joinMetadata,
                                                            final GraphTraversalSource g) throws SQLException {
        final List<Map<String, Object>> joinTraversal = getJoinTraversal(joinMetadata, g);
        final List<Object[]> results =
                joinResultsToList(joinTraversal, joinMetadata);
        final List<String> columns = new ArrayList<>();
        gremlinParseInfo.getGremlinSelectInfos().forEach(selectInfo -> columns.add(selectInfo.getMappedName()));
        return new SqlGremlinQueryResult(columns, results,
                ImmutableList.of(joinMetadata.getLeftTable(), joinMetadata.getRightTable()));
    }

    private List<Map<String, Object>> getJoinTraversal(final JoinVisitor.JoinMetadata joinMetadata,
                                                       final GraphTraversalSource g) throws SQLException {
        // Hardcode vertex-vertex join for now.
        final String leftVertexLabel = joinMetadata.getLeftTable().label;
        final String rightVertexLabel = joinMetadata.getRightTable().label;
        final String edgeLabel = joinMetadata.getJoinColumn();
        //input.getCluster().getPlanner().getRoot().getRowType().getFieldNames()

        final boolean leftInRightOut = schemaConfig.getRelationships().stream().anyMatch(
                r -> r.getInTable().toLowerCase().equals(leftVertexLabel.toLowerCase()) &&
                        r.getOutTable().toLowerCase().equals(rightVertexLabel.toLowerCase()));

        final boolean rightInLeftOut = schemaConfig.getRelationships().stream().anyMatch(
                r -> r.getInTable().toLowerCase().equals(rightVertexLabel.toLowerCase()) &&
                        r.getOutTable().toLowerCase().equals(leftVertexLabel.toLowerCase()));

        if (rightInLeftOut && leftInRightOut) {
            // TODO: Because we don't know if a bidirectional query will have enough results to reach out limit up front, we can
            // can't properly impose a limit. Right now this will return 2x the imposed LIMIT amount of queries.
            final List<Map<String, Object>> result =
                    executeJoinTraversal(joinMetadata.getLeftTable(), joinMetadata.getRightTable(),
                            joinMetadata.getEdgeTable().label, g);
            result.addAll(executeJoinTraversal(joinMetadata.getRightTable(), joinMetadata.getLeftTable(),
                    joinMetadata.getEdgeTable().label, g));
            return result;
        } else if (rightInLeftOut) {
            return executeJoinTraversal(joinMetadata.getRightTable(), joinMetadata.getLeftTable(),
                    joinMetadata.getEdgeTable().label, g);
        } else if (leftInRightOut) {
            return executeJoinTraversal(joinMetadata.getRightTable(), joinMetadata.getLeftTable(),
                    joinMetadata.getEdgeTable().label, g);
        } else {
            throw new SQLException("Failed to find in and out directionality of query.");
        }
    }

    private List<Map<String, Object>> executeJoinTraversal(final TableDef out, final TableDef in,
                                                           final String edgeLabel, final GraphTraversalSource g)
            throws SQLException {
        final boolean inValues = gremlinParseInfo.getGremlinSelectInfos().stream()
                .anyMatch(s -> in.label.toLowerCase().equals(s.getTable().toLowerCase()));
        final boolean outValues = gremlinParseInfo.getGremlinSelectInfos().stream()
                .anyMatch(s -> out.label.toLowerCase().equals(s.getTable().toLowerCase()));
        final GraphTraversal<?, ?> traversal = g.V();
        if (inValues) {
            if (outValues) {
                // Doesn't really matter what way we traverse.
                final GraphTraversal<?, ?> graphTraversal1 = __.out(edgeLabel);
                appendSelectTraversal(out, graphTraversal1);
                traversal.hasLabel(out.label).in(edgeLabel).hasLabel(in.label)
                        .project(in.label, out.label)
                        .by(getSelectTraversal(in))
                        .by(graphTraversal1);
            } else {
                // Fastest way is to start at out and traverse to in.
                traversal.hasLabel(out.label).in(edgeLabel).hasLabel(in.label)
                        .project(in.label)
                        .by(getSelectTraversal(in));
            }
        } else if (outValues) {
            // Fastest way is to start at in and traverse to out.
            traversal.hasLabel(in.label).in(edgeLabel).hasLabel(out.label)
                    .project(out.label)
                    .by(getSelectTraversal(out));

        } else {
            throw new SQLException("Failed to find values to select in join query.");
        }

        imposeLimit(traversal);
        return (List<Map<String, Object>>) traversal.toList();
    }
}
