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

import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.twilmes.sql.gremlin.processor.visitors.FieldMapVisitor;
import org.twilmes.sql.gremlin.processor.visitors.ScanVisitor;
import org.twilmes.sql.gremlin.processor.visitors.TraversalVisitor;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.schema.GremlinSchema;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.twilmes.sql.gremlin.processor.RelUtils.isConvertable;

/**
 * Executes sql queries against a Gremlin
 * <p>
 * Created by twilmes on 12/5/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinCompiler {

    private final Graph graph;
    private final SchemaConfig schemaConfig;
    private final FrameworkConfig frameworkConfig;
    private QueryPlanner queryPlanner;

    public GremlinCompiler(final Graph graph, final SchemaConfig schemaConfig) {
        this.graph = graph;
        this.schemaConfig = schemaConfig;

        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final List<RelTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        final SqlParser.Config parserConfig =
                SqlParser.configBuilder().setLex(Lex.MYSQL).build();

        frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(rootSchema.add("gremlin", new GremlinSchema(schemaConfig)))
                .traitDefs(traitDefs)
                .programs(Programs.sequence(Programs.ofRules(Programs.RULE_SET), Programs.CALC_PROGRAM))
                .build();
    }

    private RelNode getPlan(final String sql) {
        return queryPlanner.plan(sql);
    }

    public String explain(final String sql) {
        queryPlanner = new QueryPlanner(frameworkConfig);
        final RelNode node = getPlan(sql);
        return queryPlanner.explain(node);
    }

    public List<Object> execute(final String sql) {
        queryPlanner = new QueryPlanner(frameworkConfig);
        final RelNode node = getPlan(sql);
        // determine if we need to break the logical plan off and run part via Gremlin & part Calcite
        RelNode root = node;
        if (!isConvertable(node)) {
            // go until we hit a converter to find the input
            root = root.getInput(0);
            while (!isConvertable(root)) {
                root = root.getInput(0);
            }
        }

        // Get all scan chunks.  A scan chunk is a table scan and any additional operators that we've
        // pushed down like filters
        final ScanVisitor scanVisitor = new ScanVisitor();
        new RelWalker(root, scanVisitor);
        final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap = scanVisitor.getScanMap();

        // simple case, no joins
        final GraphTraversal<?, ?> traversal;
        final List<Object> rows;
        if (scanMap.size() == 1) {
            final GraphTraversal scan = TraversalBuilder.toTraversal(scanMap.values().iterator().next());
            traversal = graph.traversal().V();
            for (final Step step : (List<Step>) scan.asAdmin().getSteps()) {
                traversal.asAdmin().addStep(step);
            }

            final TableDef table = TableUtil.getTableDef(scanMap.values().iterator().next());
            final SingleQueryExecutor queryExec = new SingleQueryExecutor(node, traversal, table);
            rows = queryExec.run();
        } else {
            final FieldMapVisitor fieldMapper = new FieldMapVisitor();
            new RelWalker(root, fieldMapper);
            final TraversalVisitor traversalVisitor = new TraversalVisitor(graph.traversal(),
                    scanMap, fieldMapper.getFieldMap());
            new RelWalker(root, traversalVisitor);

            traversal = TraversalBuilder.buildMatch(graph.traversal(), traversalVisitor.getTableTraversalMap(),
                    traversalVisitor.getJoinPairs(), schemaConfig, traversalVisitor.getTableIdConverterMap());
            final JoinQueryExecutor queryExec = new JoinQueryExecutor(node, fieldMapper.getFieldMap(), traversal,
                    traversalVisitor.getTableIdMap());
            rows = queryExec.run();
        }

        return rows;
    }
}
