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

import org.twilmes.sql.gremlin.rel.GremlinTable;
import org.twilmes.sql.gremlin.rel.GremlinTableScan;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Created by twilmes on 12/4/15.
 */
public class TableUtil {
    public static String getProperty(TableDef table, String column) {
        return table.getColumn(column).getPropertyName();
    }

    public static TableDef getTableDef(List<RelNode> rels) {
        for(RelNode rel : rels) {
            if(rel instanceof GremlinTableScan) {
                final GremlinTableScan scan = (GremlinTableScan) rel;
                return scan.getGremlinTable().getTableDef();
            }
        }
        return null;
    }

    public static TableDef getTableDef(RelNode parent) {
        if(parent instanceof GremlinTableScan) {
            final GremlinTableScan scan = (GremlinTableScan) parent;
            return scan.getGremlinTable().getTableDef();
        } else {
            if(parent.getInput(0) != null) {
                return getTableDef(parent.getInput(0));
            } else {
                return null;
            }
        }
    }

    public static Object convertType(Object value, TableColumn column) {
        switch(column.getType()) {
            case "string":
                return value;
            case "integer":
                try {
                    return Integer.valueOf(String.valueOf(value));
                } catch ( NumberFormatException e ) {
                    return value;
                }
            case "long":
                try {
                    return Long.valueOf(String.valueOf(value));
                } catch ( NumberFormatException e ) {
                    return value;
                }
            case "double":
                try {
                    return Double.valueOf(String.valueOf(value));
                } catch ( NumberFormatException e ) {
                    return value;
                }
            case "boolean":
                if(value instanceof Number) {
                    if(value.equals(0)) {
                        return false;
                    } else {
                        return true;
                    }
                } else if(value instanceof String) {
                    return Boolean.valueOf((String)value);
                }
                return value;
            case "long_date":
                long longVal = ((Number) value).longValue();
                return new java.sql.Date(longVal);
            case "long_timestamp":
                longVal = ((Number) value).longValue();
                return new java.sql.Timestamp((long) value);
//                return value;
            default:
                return null;
        }
    }

    public static GremlinTable getTable(RelNode node) {
        if(node instanceof GremlinTableScan) {
            return ((GremlinTableScan) node).getGremlinTable();
        } else {
            return getTable(node.getInput(0));
        }
    }
}
