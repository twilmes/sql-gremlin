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

package org.twilmes.sql.gremlin;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.processor.QueryPlanner;
import org.twilmes.sql.gremlin.processor.RelWalker;
import org.twilmes.sql.gremlin.processor.executors.JoinQueryExecutor;
import org.twilmes.sql.gremlin.processor.executors.QueryExecutor;
import org.twilmes.sql.gremlin.processor.executors.SingleQueryExecutor;
import org.twilmes.sql.gremlin.processor.executors.SqlGremlinQueryResult;
import org.twilmes.sql.gremlin.processor.visitors.JoinVisitor;
import org.twilmes.sql.gremlin.processor.visitors.ScanVisitor;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.schema.GremlinSchema;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.twilmes.sql.gremlin.processor.RelUtils.isConvertable;

/**
 * Created by lyndonb-bq on 05/17/21.
 */
public class SqlToGremlin {
    private final GraphTraversalSource g;
    private final SchemaConfig schemaConfig;
    private final FrameworkConfig frameworkConfig;

    public SqlToGremlin(final SchemaConfig schemaConfig, final GraphTraversalSource g) throws SQLException {
        if (schemaConfig == null) {
            throw new SQLException("Schema configuration is null, cannot initialize sql-gremlin.");
        }
        this.g = g;
        this.schemaConfig = schemaConfig;
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final List<RelTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        final SqlParser.Config parserConfig =
                SqlParser.configBuilder().setLex(Lex.MYSQL).build();

        frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(rootSchema.add("gremlin", new GremlinSchema(this.schemaConfig)))
                .traitDefs(traitDefs)
                .programs(Programs.sequence(Programs.ofRules(Programs.RULE_SET), Programs.CALC_PROGRAM))
                .build();
    }

    public String explain(final String sql) {
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);
        queryPlanner.plan(sql);
        final RelNode node = queryPlanner.getTransform();
        return queryPlanner.explain(node);
    }

    public SqlGremlinQueryResult execute(final String sql) throws SQLException {
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);
        queryPlanner.plan(sql);
        final RelNode node = queryPlanner.getTransform();
        SqlNode sqlNode = queryPlanner.getValidate();
        // determine if we need to break the logical plan off and run part via Gremlin & part Calcite
        RelNode root = node;
        if(!isConvertable(node)) {
            // go until we hit a converter to find the input
            root = root.getInput(0);
            while(!isConvertable(root)) {
                root = root.getInput(0);
            };
        }

        // Get all scan chunks.  A scan chunk is a table scan and any additional operators that we've
        // pushed down like filters
        final ScanVisitor scanVisitor = new ScanVisitor();
        RelWalker.executeWalk(root, scanVisitor);
        final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap = scanVisitor.getScanMap();

        return null;
    }

    public SqlGremlinQueryResult execute2(final String sql) throws SQLException {
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);
        queryPlanner.plan(sql);
        final RelNode node = queryPlanner.getTransform();
        final SqlNode sqlNode = queryPlanner.getValidate();
        int limit = -1;
        final List<GremlinSelectInfo> gremlinSelectInfoList = new ArrayList<>();
        if (sqlNode instanceof SqlSelect) {
            final SqlSelect sqlSelect = (SqlSelect) sqlNode;
            final SqlNode sqlFetch = sqlSelect.getFetch();
            if (sqlFetch != null) {
                if (sqlFetch instanceof SqlNumericLiteral) {
                    limit = ((SqlNumericLiteral) sqlFetch).bigDecimalValue().intValue();
                }
            }

            final SqlNodeList sqlNodeList = sqlSelect.getSelectList();
            for (final SqlNode sqlSelectNode : sqlNodeList.getList()) {
                if (sqlSelectNode instanceof SqlIdentifier) {
                    final SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlSelectNode;
                    final String table = sqlIdentifier.names.get(0);
                    final String column = sqlIdentifier.names.get(1);
                    gremlinSelectInfoList.add(new GremlinSelectInfo(table, column, column));
                } else if (sqlSelectNode instanceof SqlBasicCall) {
                    final SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlSelectNode;
                    final SqlNode[] sqlBasicCallNodes = sqlBasicCall.getOperands();
                    if (sqlBasicCallNodes.length > 0) {
                        if (sqlBasicCallNodes[0] instanceof SqlIdentifier) {
                            String mappedName = null;
                            final SqlOperator sqlOperator = sqlBasicCall.getOperator();
                            if (sqlOperator.kind.lowerName.equals("as")) {
                                if (sqlBasicCallNodes.length > 1) {
                                    if (sqlBasicCallNodes[1] instanceof SqlIdentifier) {
                                        mappedName = ((SqlIdentifier) sqlBasicCallNodes[1]).names.get(0);
                                    }
                                }
                            }
                            final String table = ((SqlIdentifier) sqlBasicCallNodes[0]).names.get(0);
                            final String column = ((SqlIdentifier) sqlBasicCallNodes[0]).names.get(1);
                            gremlinSelectInfoList.add(new GremlinSelectInfo(table, column, mappedName));
                        }

                    }
                }
            }
        }

        final GremlinParseInfo gremlinParseInfo = new GremlinParseInfo(limit, gremlinSelectInfoList);

        // Determine if we need to break the logical plan off and run part via Gremlin & part Calcite
        RelNode root = node;
        if (!isConvertable(node)) {
            // go until we hit a converter to find the input
            root = root.getInput(0);
            while (!isConvertable(root)) {
                root = root.getInput(0);
            }
        }

        // Get all scan chunks.  A scan chunk is a table scan and any additional operators that we've
        // pushed down like filters.
        final ScanVisitor scanVisitor = new ScanVisitor();
        RelWalker.executeWalk(root, scanVisitor);
        final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap = scanVisitor.getScanMap();

        // Simple case, no joins.
        final QueryExecutor queryExecutor;
        if (scanMap.size() == 1) {
            final TableDef table = TableUtil.getTableDef(scanMap.values().iterator().next());
            if (table == null) {
                throw new SQLException("Failed to find table definition.");
            }

            queryExecutor = new SingleQueryExecutor(schemaConfig, gremlinParseInfo, table);
        } else {
            final JoinVisitor joinVisitor = new JoinVisitor(schemaConfig, gremlinSelectInfoList);
            RelWalker.executeWalk(root, joinVisitor);

            queryExecutor = new JoinQueryExecutor(schemaConfig, gremlinParseInfo, joinVisitor.getJoinMetadata());
        }
        return queryExecutor.handle(g);
    }

    @AllArgsConstructor
    @Getter
    public class GremlinParseInfo {
        int limit;
        List<GremlinSelectInfo> gremlinSelectInfos;
    }

    @AllArgsConstructor
    @Getter
    public class GremlinSelectInfo {
        String table;
        String column;
        String mappedName;
    }
}
