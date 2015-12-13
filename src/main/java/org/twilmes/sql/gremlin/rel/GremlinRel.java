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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by twilmes on 9/25/15.
 */
public interface GremlinRel extends RelNode {
    void implement(Implementor implementor);

    /** Calling convention for relational operations that occur in Gremlin. */
    Convention CONVENTION = new Convention.Impl("GREMLIN", GremlinRel.class);

    /** Callback for the implementation process that converts a tree of
     * {@link GremlinRel} nodes into a Gremlin query. */
    class Implementor {
        final List<Pair<String, String>> list =
                new ArrayList<Pair<String, String>>();

        RelOptTable table;
        GremlinTable gremlinTable;
        RexNode filterRoot;

        public void setFilter(RexNode filterRoot) {
            this.filterRoot = filterRoot;
        }

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((GremlinRel) input).implement(this);
        }
    }
}
