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

/**
 * Created by twilmes on 10/10/15.
 */
public class TableRelationship {
    private String outTable;
    private String inTable;
    private String edgeLabel;
    private String fkTable;

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
    }

    public String getOutTable() {
        return outTable;
    }

    public void setOutTable(String outTable) {
        this.outTable = outTable;
    }

    public String getInTable() {
        return inTable;
    }

    public void setInTable(String inTable) {
        this.inTable = inTable;
    }

    public String getFkTable() { return this.fkTable != null ? this.fkTable : this.outTable; }

    public void setFkTable(String fkTable) { this.fkTable = fkTable; }

    public Boolean isBetween(String t1, String t2) {
        if(t1.equalsIgnoreCase(inTable) && t2.equalsIgnoreCase(outTable)) {
            return true;
        } else if (t2.equalsIgnoreCase(inTable) && t1.equalsIgnoreCase(outTable)) {
            return true;
        } else {
            return false;
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
