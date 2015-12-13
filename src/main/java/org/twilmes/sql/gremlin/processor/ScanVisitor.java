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

package org.twilmes.sql.gremlin.processor;

import org.twilmes.sql.gremlin.rel.GremlinToEnumerableConverter;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twilmes on 11/28/15.
 */
public class ScanVisitor implements RelVisitor {

    private List<RelNode> stack = new ArrayList<>();
    private Map<GremlinToEnumerableConverter, List<RelNode>> scanMap = new HashMap<>();

    public Map<GremlinToEnumerableConverter, List<RelNode>> getScans() {
        return scanMap;
    }

    @Override
    public void visit(RelNode node) {
        if(node instanceof GremlinToEnumerableConverter) {
            scanMap.put((GremlinToEnumerableConverter) node, stack);
            stack = new ArrayList<>();
            return;
        }
        stack.add(node);
    }
}
