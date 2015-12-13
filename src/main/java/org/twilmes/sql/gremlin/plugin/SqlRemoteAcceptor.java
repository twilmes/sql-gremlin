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

package org.twilmes.sql.gremlin.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.twilmes.sql.gremlin.processor.GremlinCompiler;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.io.File;
import java.util.List;

public class SqlRemoteAcceptor implements RemoteAcceptor {

    private final Groovysh shell;
    private GraphTraversalSource g;
    private GremlinCompiler compiler;

    public SqlRemoteAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public Object connect(final List<String> args) throws RemoteException {
        // Driver class isn't automatically loaded on plugin startup so
        // we do it the old fashioned way here.  Not sure why Calcite
        // needs this even though we're not using the jdbc driver yet.
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (args.size() != 2) {
            throw new IllegalArgumentException("Usage: :remote connect " +
                    SqlGremlinPlugin.NAME + " <variable name of graph or graph traversal source> <schema file>");
        }
        final Object graphOrTraversalSource = this.shell.getInterp().getContext().getVariable(args.get(0));
        final String schemaFn = (String) this.shell.getInterp().getContext().getVariable(args.get(1));
        if (graphOrTraversalSource instanceof Graph) {
            this.g = ((Graph) graphOrTraversalSource).traversal();
        } else {
            this.g = (GraphTraversalSource) graphOrTraversalSource;
        }
        // Read the schema config in and intialize the compiler.
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final SchemaConfig schemaConfig = mapper.readValue(new File(schemaFn),
                    SchemaConfig.class);
            compiler = new GremlinCompiler(g.getGraph().get(), schemaConfig);
        } catch (final Exception e) {
            throw new RemoteException(e);
        }

        return this;
    }

    @Override
    public Object configure(final List<String> args) throws RemoteException {
        return null;
    }

    @Override
    public Object submit(final List<String> args) throws RemoteException {
        try {
            final String query = RemoteAcceptor.getScript(String.join(" ", args), this.shell);

            // if the query starts with 'explain', return the plan instead of the results
            if(query.toLowerCase().startsWith("explain")) {
                int explainIndex = query.toLowerCase().indexOf("explain");
                return compiler.explain(query.substring(explainIndex + 7, query.length()));
            }

            return compiler.execute(query);
        } catch (final Exception e) {
            throw new RemoteException(e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "SQL[" + this.g + "]";
    }
}
