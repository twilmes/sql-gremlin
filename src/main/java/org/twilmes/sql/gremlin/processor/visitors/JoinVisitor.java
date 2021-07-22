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

package org.twilmes.sql.gremlin.processor.visitors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.rel.RelNode;
import org.twilmes.sql.gremlin.ParseException;
import org.twilmes.sql.gremlin.rel.GremlinTableScan;
import org.twilmes.sql.gremlin.schema.SchemaConfig;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableRelationship;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.twilmes.sql.gremlin.schema.GremlinSchema.getTableDef;

/**
 * Created by lyndonb-bq on 05/17/21.
 */
public class JoinVisitor implements RelVisitor {
    private final SchemaConfig schemaConfig;
    JoinMetadata joinMetadata = null;


    public JoinVisitor(final SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    @Override
    public void visit(final RelNode node) {
        if (!(node instanceof EnumerableHashJoin)) {
            return;
        }

        final EnumerableHashJoin join = (EnumerableHashJoin) node;
        if (!join.getJoinType().lowerName.equals("inner")) {
            throw new ParseException("Only inner joins are supported.");
        }
        final RelNode left = join.getLeft();
        final TableDef leftTableDef = ((GremlinTableScan) left.getInput(0)).getGremlinTable().getTableDef();
        final RelNode right = join.getRight();
        final TableDef rightTableDef = ((GremlinTableScan) right.getInput(0)).getGremlinTable().getTableDef();
        final String leftKey = left.getRowType().getFieldList().get(join.analyzeCondition().leftKeys.get(0)).getKey();
        final String rightKey =
                right.getRowType().getFieldList().get(join.analyzeCondition().rightKeys.get(0)).getKey();

        if (!leftKey.equals(rightKey)) {
            throw new ParseException("Join must be performed on equal keys.");
        }
        final Optional<TableRelationship>
                tableRelationshipOptional = schemaConfig.getRelationships().stream()
                .filter(rel -> rel.getEdgeLabel().toLowerCase().equals(leftKey.replace("_ID", "").toLowerCase()))
                .findFirst();
        if (!tableRelationshipOptional.isPresent()) {
            throw new ParseException(
                    "Failed to find relationship between vertexes in relationship table, cannot perform join.");
        }

        if (joinMetadata != null) {
            throw new ParseException("Multiple joins are not supported.");
        }

        joinMetadata = new JoinMetadata(leftTableDef, rightTableDef, getTableDef(tableRelationshipOptional.get()),
                tableRelationshipOptional.get().getEdgeLabel(), join.getJoinType().lowerName);
    }

    public JoinMetadata getJoinMetadata() {
        if (joinMetadata == null) {
            throw new ParseException("No join found.");
        }
        return joinMetadata;
    }

    @AllArgsConstructor
    @Getter
    public static class JoinMetadata {
        private final TableDef leftTable;
        private final TableDef rightTable;
        private final TableDef edgeTable;
        private final String joinColumn;
        private final String joinType;

        public String getJoinColumn() {
            return joinColumn.replace("_ID", "");
        }
    }
}
