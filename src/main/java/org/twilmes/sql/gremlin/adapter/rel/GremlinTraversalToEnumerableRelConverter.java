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

package org.twilmes.sql.gremlin.adapter.rel;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;
import java.util.List;

/**
 * Created by twilmes on 11/26/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinTraversalToEnumerableRelConverter extends ConverterImpl
        implements EnumerableRel {
    public GremlinTraversalToEnumerableRelConverter(
            final RelOptCluster cluster,
            final RelTraitSet traits,
            final RelNode input, final RelDataType rowType) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
        this.rowType = rowType;
    }

    @Override
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return new GremlinTraversalToEnumerableRelConverter(
                getCluster(), traitSet, sole(inputs), rowType);
    }

    @Override
    public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
        final GremlinTraversalRel.Implementor gremlinImplementor =
                new GremlinTraversalRel.Implementor();
        gremlinImplementor.visitChild(0, getInput());

        final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(),
                getRowType(), pref.preferArray());

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(GremlinTraversalScan.class,
                                "scan")));
    }

}
