package org.twilmes.sql.gremlin;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.twilmes.sql.gremlin.processor.FieldMapVisitor;
import org.twilmes.sql.gremlin.processor.JoinQueryExecutor;
import org.twilmes.sql.gremlin.processor.QueryPlanner;
import org.twilmes.sql.gremlin.processor.RelWalker;
import org.twilmes.sql.gremlin.processor.ScanVisitor;
import org.twilmes.sql.gremlin.processor.SingleQueryExecutor;
import org.twilmes.sql.gremlin.processor.TraversalBuilder;
import org.twilmes.sql.gremlin.processor.TraversalVisitor;
import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.twilmes.sql.gremlin.schema.GremlinSchema;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.twilmes.sql.gremlin.processor.RelUtils.isConvertable;

public class SqlToGremlin {
    private final GraphTraversalSource g = null; // TODO: g = traversal().withRemote(remoteConnection);
    private final SchemaConfig schemaConfig;
    private final FrameworkConfig frameworkConfig;
    private QueryPlanner queryPlanner;

    public SqlToGremlin(SchemaConfig schemaConfig) {
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


    public String runQuery(final String query) throws SQLException {
        final ObjectMapper mapper = new ObjectMapper();
        final org.twilmes.sql.gremlin.schema.SchemaConfig schemaConfig;
        try {
            schemaConfig = mapper.readValue(new File("output.json"), org.twilmes.sql.gremlin.schema.SchemaConfig.class);
        } catch (Exception e) {
            throw new SQLException("Error reading the schema file.", e);
        }

        final List<Object> queryResult = execute(query);
        return "";
    }

    private RelNode getPlan(String sql) {
        return queryPlanner.plan(sql);
    }

    public String explain(String sql) {
        queryPlanner = new QueryPlanner(frameworkConfig);
        final RelNode node = getPlan(sql);
        return queryPlanner.explain(node);
    }

    public List<Object> execute(String sql) {
        queryPlanner = new QueryPlanner(frameworkConfig);
        final RelNode node = getPlan(sql);

        // Determine if we need to break the logical plan off and run part via Gremlin & part Calcite/
        RelNode root = node;
        if(!isConvertable(node)) {
            // go until we hit a converter to find the input
            root = root.getInput(0);
            while(!isConvertable(root)) {
                root = root.getInput(0);
            };
        }

        // Get all scan chunks.  A scan chunk is a table scan and any additional operators that we've
        // pushed down like filters.
        final ScanVisitor scanVisitor = new ScanVisitor();
        new RelWalker(root, scanVisitor);
        final Map<GremlinToEnumerableConverter, List<RelNode>> scanMap = scanVisitor.getScans();

        // Simple case, no joins.
        final GraphTraversal<?, ?> traversal;
        List<Object> rows;
        if (scanMap.size() == 1) {
            final GraphTraversal<?, ?> scan = TraversalBuilder.toTraversal(scanMap.values().iterator().next());
            // TODO: If graph is null use traversal();
            traversal = g.V();
            for (Step<?, ?> step : scan.asAdmin().getSteps()) {
                traversal.asAdmin().addStep(step);
            }

            final TableDef table = TableUtil.getTableDef(scanMap.values().iterator().next());
            assert table != null; // TODO: Do something different.

            final SingleQueryExecutor queryExec = new SingleQueryExecutor(node, traversal, table);
            rows = queryExec.run();
        } else {
            final FieldMapVisitor fieldMapper = new FieldMapVisitor();
            new RelWalker(root, fieldMapper);
            final TraversalVisitor traversalVisitor = new TraversalVisitor(g,
                    scanMap, fieldMapper.getFieldMap());
            new RelWalker(root, traversalVisitor);

            traversal = TraversalBuilder.buildMatch(g, traversalVisitor.getTableTraversalMap(),
                    traversalVisitor.getJoinPairs(), schemaConfig, traversalVisitor.getTableIdConverterMap());
            final JoinQueryExecutor queryExec = new JoinQueryExecutor(node, fieldMapper.getFieldMap(), traversal,
                    traversalVisitor.getTableIdMap());
            rows = queryExec.run();
        }

        return rows;
    }

}
