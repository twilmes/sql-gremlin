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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by twilmes on 9/22/15.
 */
public class TableDef {
    public String tableName;
    public String label;
    public Boolean isVertex;

    public Map<String, TableColumn> columns = new HashMap<>();
    public Map<String, String> outEdgeMap = new HashMap<>();
    public Map<String, String> inEdgeMap = new HashMap<>();

    public TableColumn getColumn(String column) {
        final Optional<TableColumn> res = this.columns.values().
                stream().filter(col -> column.toLowerCase().equals(col.getName().toLowerCase())).
                findFirst();
        if(res.isPresent()) {
            return res.get();
        } else {
            return null;
        }
    }
}
