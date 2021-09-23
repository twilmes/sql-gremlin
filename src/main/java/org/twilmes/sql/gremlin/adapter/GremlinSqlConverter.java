package org.twilmes.sql.gremlin.adapter;

import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.nodes.GremlinSqlSelect;
import org.twilmes.sql.gremlin.processor.QueryPlanner;
import org.twilmes.sql.gremlin.schema.GremlinSchema;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GremlinSqlConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelect.class);
    private final SqlParser.Config parserConfig =
            SqlParser.configBuilder().setLex(Lex.MYSQL).build();
    private final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    private final List<RelTraitDef> traitDefs = new ArrayList<>();
    private final FrameworkConfig frameworkConfig;
    private final GraphTraversalSource g;
    private final SchemaConfig schemaConfig;
    private final GremlinSqlMetadata gremlinSqlMetadata;

    GremlinSqlConverter(final SchemaConfig schemaConfig, final GraphTraversalSource g) {
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
        this.gremlinSqlMetadata = new GremlinSqlMetadata(g, schemaConfig);
        GremlinSqlFactory.setSqlMetadata(gremlinSqlMetadata);
    }

    public String getStringTraversal(final String query) throws SQLException {
        // Not sure if this can be re-used?
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);

        queryPlanner.plan(query);
        final RelNode node = queryPlanner.getTransform();
        final SqlNode sqlNode = queryPlanner.getValidate();

        if (sqlNode instanceof SqlSelect) {
            final GremlinSqlSelect gremlinSqlSelect = new GremlinSqlSelect((SqlSelect)sqlNode, gremlinSqlMetadata);
            return GroovyTranslator.of("g").
                    translate(gremlinSqlSelect.executeTraversal().asAdmin().getBytecode());
        } else {
            throw new SQLException("Only sql select statements are supported right now.");
        }
    }
}
