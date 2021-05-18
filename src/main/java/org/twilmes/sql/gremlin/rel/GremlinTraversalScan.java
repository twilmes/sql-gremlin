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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import java.util.List;

/**
 * Created by twilmes on 11/22/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinTraversalScan extends AbstractRelNode implements GremlinTraversalRel {
    private static List<Object> rows;

    public GremlinTraversalScan(final RelOptCluster cluster, final RelTraitSet traitSet, final RelDataType rowType, final List<Object> rows) {
        super(cluster, traitSet);
        this.rowType = rowType;
        GremlinTraversalScan.rows = rows;
    }

    public static Enumerable<Object[]> scan() {
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new GremlinEnumerator(rows);
            }
        };
    }

    @Override
    public void implement(final Implementor implementor) {
        implementor.rows = rows;
    }
}
