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

package org.twilmes.sql.gremlin.adapter.converter;

import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelect;
import org.twilmes.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import org.twilmes.sql.gremlin.adapter.converter.schema.GremlinSchema;
import org.twilmes.sql.gremlin.adapter.converter.schema.SchemaConfig;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This module is the entry point of the SqlGremlin conversion.
 *
 * @author Lyndon Bauto (lyndonb@bitquilltech.com)
 */
public class SqlConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelect.class);
    private final SqlParser.Config parserConfig =
            SqlParser.configBuilder().setLex(Lex.MYSQL).build();
    private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    private final List<RelTraitDef> traitDefs = new ArrayList<>();
    private final FrameworkConfig frameworkConfig;
    private final GraphTraversalSource g;
    private final SchemaConfig schemaConfig;

    public SqlConverter(final SchemaConfig schemaConfig, final GraphTraversalSource g) {
        this.schemaConfig = schemaConfig;
        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        this.frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(rootSchema.add("gremlin", new GremlinSchema(schemaConfig)))
                .traitDefs(traitDefs)
                .programs(Programs.sequence(Programs.ofRules(Programs.RULE_SET), Programs.CALC_PROGRAM))
                .build();
        this.g = g;
    }

    // NOT THREAD SAFE
    public SqlGremlinQueryResult executeQuery(final String query) throws SQLException {
        final SqlMetadata sqlMetadata = new SqlMetadata(g, schemaConfig);
        GremlinSqlFactory.setSqlMetadata(sqlMetadata);
        // Not sure if this can be re-used?
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);

        queryPlanner.plan(query);
        final RelNode node = queryPlanner.getTransform();
        final SqlNode sqlNode = queryPlanner.getValidate();

        if (sqlNode instanceof SqlSelect) {
            final GremlinSqlSelect gremlinSqlSelect = GremlinSqlFactory.createSelect((SqlSelect) sqlNode, g);
            return gremlinSqlSelect.executeTraversal();
        } else {
            throw new SQLException("Only sql select statements are supported right now.");
        }
    }

    private GraphTraversal<?, ?> getGraphTraversal(final String query) throws SQLException {
        final SqlMetadata sqlMetadata = new SqlMetadata(g, schemaConfig);
        GremlinSqlFactory.setSqlMetadata(sqlMetadata);
        // Not sure if this can be re-used?
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);

        queryPlanner.plan(query);
        final RelNode node = queryPlanner.getTransform();
        final SqlNode sqlNode = queryPlanner.getValidate();

        if (sqlNode instanceof SqlSelect) {
            final GremlinSqlSelect gremlinSqlSelect = GremlinSqlFactory.createSelect((SqlSelect) sqlNode, g);
            return gremlinSqlSelect.generateTraversal();
        } else {
            throw new SQLException("Only sql select statements are supported right now.");
        }
    }

    public String getStringTraversal(final String query) throws SQLException {
        return GroovyTranslator.of("g").translate(getGraphTraversal(query).asAdmin().getBytecode());
    }

    @Getter
    private static class QueryPlanner {
        private final Planner planner;
        private SqlNode parse;
        private SqlNode validate;
        private RelRoot convert;
        private RelTraitSet traitSet;
        private RelNode transform;

        public QueryPlanner(final FrameworkConfig frameworkConfig) {
            this.planner = Frameworks.getPlanner(frameworkConfig);
        }

        public void plan(final String sql) throws SQLException {
            try {
                parse = planner.parse(sql);
                validate = planner.validate(parse);
                convert = planner.rel(validate);
                traitSet = planner.getEmptyTraitSet()
                        .replace(EnumerableConvention.INSTANCE);
                transform = planner.transform(0, traitSet, convert.project());
            } catch (final Exception e) {
                throw new SQLException(String.format("Error parsing: \"%s\". Error: \"%s\".", sql, e), e);
            }
        }
    }
}
