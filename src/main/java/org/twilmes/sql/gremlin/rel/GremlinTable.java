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

package org.twilmes.sql.gremlin.rel;

import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by twilmes on 9/22/15.
 */
public class GremlinTable extends AbstractQueryableTable implements TranslatableTable {
    private final TableDef tableDef;

    public GremlinTable(TableDef tableDef) {
        super(Object[].class);
        this.tableDef = tableDef;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return null;
    }

    public TableDef getTableDef() {
        return tableDef;
    }

    public RelNode toRel(
            RelOptTable.ToRelContext context,
            RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        int fieldCount = tableDef.columns.size();
        int[] fields = new int[fieldCount];
        for(int i = 0;i < fieldCount; i++) {
            fields[i] = i;
        }
        return new GremlinTableScan(cluster, cluster.traitSetOf(GremlinRel.CONVENTION),
                relOptTable, this, fields);
    }

    public Enumerable<Object> find() {
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();

        for(Map.Entry<String, TableColumn> entry : tableDef.columns.entrySet()) {
            names.add(entry.getKey());
            types.add(relDataTypeFactory.createJavaType(
                    getType(entry.getValue().getType())));
        }

        return relDataTypeFactory.createStructType(Pair.zip(names, types));
    }

    private Class getType(String className) {
        switch(className) {
            case "string":
                return String.class;
            case "integer":
                return Integer.class;
            case "double":
                return Double.class;
            case "long":
                return Long.class;
            case "boolean":
                return Boolean.class;
            case "date":
            case "long_date":
                return java.sql.Date.class;
            case "timestamp":
            case "long_timestamp":
                return java.sql.Timestamp.class;
            default:
                return null;
        }
    }
}
