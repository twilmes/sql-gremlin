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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.twilmes.sql.gremlin.rel.GremlinFilter;
import org.twilmes.sql.gremlin.rel.GremlinTableScan;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.util.FilterTranslator;
import java.util.List;

/**
 * Builds a Gremlin traversal given a list of supported Rel nodes.
 * <p>
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class TraversalBuilder {
    public static GraphTraversal<?, ?> toTraversal(final List<RelNode> relList) {
        final GraphTraversal<?, ?> traversal = __.identity();
        TableDef tableDef = null;
        for (final RelNode rel : relList) {
            if (rel instanceof GremlinTableScan) {
                final GremlinTableScan tableScan = (GremlinTableScan) rel;
                tableDef = tableScan.getGremlinTable().getTableDef();
                traversal.hasLabel(tableDef.label);
            }
            if (rel instanceof GremlinFilter) {
                final GremlinFilter filter = (GremlinFilter) rel;
                final RexNode condition = filter.getCondition();
                final FilterTranslator translator = new FilterTranslator(tableDef, filter.getRowType().getFieldNames());
                final GraphTraversal<?, ?> predicates = translator.translateMatch(condition);
                for (final Step<?, ?> step : predicates.asAdmin().getSteps()) {
                    traversal.asAdmin().addStep(step);
                }
            }
        }
        return traversal;
    }

    public static void appendTraversal(final List<RelNode> relList, final GraphTraversal<?, ?> traversal) {
        TableDef tableDef = null;
        for (final RelNode rel : relList) {
            if (rel instanceof GremlinTableScan) {
                final GremlinTableScan tableScan = (GremlinTableScan) rel;
                tableDef = tableScan.getGremlinTable().getTableDef();
                traversal.hasLabel(tableDef.label);
            }
            if (rel instanceof GremlinFilter) {
                final GremlinFilter filter = (GremlinFilter) rel;
                final RexNode condition = filter.getCondition();
                final FilterTranslator translator = new FilterTranslator(tableDef, filter.getRowType().getFieldNames());
                final GraphTraversal<?, ?> predicates = translator.translateMatch(condition);
                for (final Step<?, ?> step : predicates.asAdmin().getSteps()) {
                    traversal.asAdmin().addStep(step);
                }
            }
        }
    }
}