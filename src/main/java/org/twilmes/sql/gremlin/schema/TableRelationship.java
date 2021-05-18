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

import lombok.Getter;
import lombok.Setter;

/**
 * Created by twilmes on 10/10/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
@Setter
@Getter
public class TableRelationship {
    private String outTable;
    private String inTable;
    private String edgeLabel;
    private String fkTable;

    // Override with null check.
    public String getFkTable() {
        return this.fkTable != null ? this.fkTable : this.outTable;
    }

    public Boolean isBetween(final String t1, final String t2) {
        if (t1.equalsIgnoreCase(inTable) && t2.equalsIgnoreCase(outTable)) {
            return true;
        } else {
            return t2.equalsIgnoreCase(inTable) && t1.equalsIgnoreCase(outTable);
        }
    }

    @Override
    public String toString() {
        return "TableRelationship{" +
                "outTable='" + outTable + '\'' +
                ", inTable='" + inTable + '\'' +
                ", edgeLabel='" + edgeLabel + '\'' +
                ", fkTable='" + fkTable + '\'' +
                '}';
    }
}
