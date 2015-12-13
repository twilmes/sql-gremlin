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

import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.twilmes.sql.gremlin.util.SqlFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SqlGremlinPlugin extends AbstractGremlinPlugin {

    final static String NAME = "sql.gremlin";

    private static final String IMPORT = "import ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + SqlFactory.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.of(new SqlRemoteAcceptor(this.shell));
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor)
            throws IllegalEnvironmentException, PluginInitializationException {
        super.pluginTo(pluginAcceptor);
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(PluginAcceptor pluginAcceptor)
            throws IllegalEnvironmentException, PluginInitializationException {
    }
}
