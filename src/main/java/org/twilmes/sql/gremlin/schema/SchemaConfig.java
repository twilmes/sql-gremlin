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

package org.twilmes.sql.gremlin.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twilmes on 10/10/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class SchemaConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConfig.class);

    List<TableConfig> tables;
    List<TableRelationship> relationships;

    public List<TableConfig> getTables() {
        LOGGER.debug("getTables()");
        tables.forEach(t -> {
            LOGGER.debug(String.format("Table: %s", t.getName()));
            t.getColumns().forEach(c -> {
                LOGGER.debug(String.format("- Column: %s", c.getName()));
            });
        });
        return tables;
    }

    public void setTables(final List<TableConfig> tables) {
        this.tables = tables;
    }

    public List<TableRelationship> getRelationships() {
        return relationships;
    }

    public void setRelationships(final List<TableRelationship> relationships) {
        this.relationships = relationships;
    }

    public Map<String, TableConfig> getTableMap() {
        final Map<String, TableConfig> tableMap = new HashMap<>();
        for (final TableConfig table : tables) {
            tableMap.put(table.getName(), table);
        }
        return tableMap;
    }
}
