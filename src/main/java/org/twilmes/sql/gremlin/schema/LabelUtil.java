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

import org.apache.tinkerpop.gremlin.structure.Direction;
import java.util.List;
import java.util.Optional;

/**
 * Created by twilmes on 11/13/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class LabelUtil {

    public static LabelInfo getLabel(final TableDef t1, final TableDef t2, final SchemaConfig config) {
        final List<TableRelationship> relationships = config.getRelationships();

        Optional<TableRelationship> relationship = null;
        if (t1.isVertex && !t2.isVertex) {
            relationship = relationships.stream().filter(tableRel ->
                    tableRel.getOutTable().equals(t1.label) && tableRel.getEdgeLabel().equals(t2.label)).findFirst();
            if (relationship.isPresent()) {
                return new LabelInfo(t2.label, Direction.OUT);
            } else {
                return new LabelInfo(t2.label, Direction.IN);
            }
        }
        if (!t1.isVertex && t2.isVertex) {
            relationship = relationships.stream().filter(tableRel ->
                    tableRel.getInTable().equals(t2.label) && tableRel.getEdgeLabel().equals(t1.label)).findFirst();
            if (relationship.isPresent()) {
                return new LabelInfo(t1.label, Direction.OUT);
            } else {
                return new LabelInfo(t1.label, Direction.IN);
            }
        }

        relationship = relationships.stream().filter(tableRel ->
                tableRel.getOutTable().equals(t1.label) && tableRel.getInTable().equals(t2.label)).findFirst();
        if (relationship.isPresent()) {
            return new LabelInfo(relationship.get().getEdgeLabel(), Direction.OUT);
        }

        relationship = relationships.stream().filter(tableRel ->
                tableRel.getInTable().equals(t1.label) && tableRel.getOutTable().equals(t2.label)).findFirst();
        if (relationship.isPresent()) {
            return new LabelInfo(relationship.get().getEdgeLabel(), Direction.IN);
        }

        return null;
    }
}
