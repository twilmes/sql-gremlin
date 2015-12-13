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

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Arrays;
import java.util.List;

/**
 * Created by twilmes on 9/25/15.
 */
public class GremlinTableScan extends TableScan implements GremlinRel {
    private final GremlinTable gremlinTable;
    private final int[] fields;

    protected GremlinTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                               RelOptTable table, GremlinTable gremlinTable, int[] fields) {
        super(cluster, traitSet, table);
        this.gremlinTable = gremlinTable;
        this.fields = fields;

        assert gremlinTable != null;
        assert getConvention() == CONVENTION;
    }

    public GremlinTable getGremlinTable() {
        return gremlinTable;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.FieldInfoBuilder builder =
                getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(GremlinToEnumerableConverterRule.INSTANCE);
        for (RelOptRule rule : GremlinRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.gremlinTable = gremlinTable;
        implementor.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GremlinTableScan that = (GremlinTableScan) o;

        if (!Arrays.equals(fields, that.fields)) return false;
        if (!gremlinTable.equals(that.gremlinTable)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = gremlinTable.hashCode();
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
