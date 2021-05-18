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

import org.apache.calcite.rel.RelNode;
import org.twilmes.sql.gremlin.rel.GremlinTable;
import org.twilmes.sql.gremlin.rel.GremlinTableScan;
import java.util.List;

/**
 * Created by twilmes on 12/4/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class TableUtil {
    public static String getProperty(final TableDef table, final String column) {
        final TableColumn tableColumn = table.getColumn(column);
        final String propName = tableColumn.getPropertyName();
        return propName;
    }

    public static TableDef getTableDef(final List<RelNode> rels) {
        for (final RelNode rel : rels) {
            if (rel instanceof GremlinTableScan) {
                final GremlinTableScan scan = (GremlinTableScan) rel;
                return scan.getGremlinTable().getTableDef();
            }
        }
        return null;
    }

    public static TableDef getTableDef(final RelNode parent) {
        if (parent instanceof GremlinTableScan) {
            final GremlinTableScan scan = (GremlinTableScan) parent;
            return scan.getGremlinTable().getTableDef();
        } else {
            if (parent.getInput(0) != null) {
                return getTableDef(parent.getInput(0));
            } else {
                return null;
            }
        }
    }

    public static Object convertType(final Object value, final TableColumn column) {
        // TODO: Switch to look up table.
        switch (column.getType()) {
            case "string":
                return value;
            case "integer":
                return ((Number) value).intValue();
            case "long":
                return ((Number) value).longValue();
            case "double":
                return ((Number) value).doubleValue();
            case "boolean":
                if (value instanceof Number) {
                    return !value.equals(0);
                } else if (value instanceof String) {
                    return Boolean.valueOf((String) value);
                }
                return value;
            case "long_date": {
                final long longVal = ((Number) value).longValue();
                return new java.sql.Date(longVal);
            }
            case "long_timestamp": {
                return new java.sql.Timestamp((long) value);
            }
            default:
                return null;
        }
    }

    public static GremlinTable getTable(final RelNode node) {
        if (node instanceof GremlinTableScan) {
            return ((GremlinTableScan) node).getGremlinTable();
        } else {
            return getTable(node.getInput(0));
        }
    }
}
