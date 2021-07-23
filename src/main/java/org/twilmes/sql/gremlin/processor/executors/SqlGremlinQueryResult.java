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

package org.twilmes.sql.gremlin.processor.executors;

import lombok.Getter;
import org.twilmes.sql.gremlin.schema.TableColumn;
import org.twilmes.sql.gremlin.schema.TableDef;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class SqlGremlinQueryResult {
    private final List<String> columns;
    private final List<String> columnTypes = new ArrayList<>();
    private final Object assertEmptyLock = new Object();
    private final BlockingQueue<List<Object>> blockingQueueRows = new LinkedBlockingQueue<>();
    private boolean isEmpty = false;
    private Thread currThread = null;

    SqlGremlinQueryResult(final List<String> columns, final List<TableDef> tableConfigs) {
        this.columns = columns;

        for (final String column : columns) {
            TableColumn col = null;
            for (final TableDef tableConfig : tableConfigs) {
                if (tableConfig.columns.containsKey(column)) {
                    col = tableConfig.getColumn(column);
                    break;
                }
            }
            columnTypes.add((col == null || col.getType() == null) ? "string" : col.getType());
        }
    }

    public void assertIsEmpty() {
        synchronized (assertEmptyLock) {
            if (currThread != null && blockingQueueRows.size() == 0) {
                currThread.interrupt();
            }
            isEmpty = true;
        }
    }

    public void addResults(final List<List<Object>> rows) {
        blockingQueueRows.addAll(rows);
    }

    public Object getResult() throws SQLException {
        try {
            synchronized (assertEmptyLock) {
                // Pass current thread in, and interrupt in assertIsEmpty.
                this.currThread = Thread.currentThread();
                if (isEmpty && blockingQueueRows.size() == 0) {
                    throw new SQLException("No more results.");
                }
            }
            return this.blockingQueueRows.take();
        } catch (final InterruptedException ignored) {
            throw new SQLException("No more results.");
        }
    }
}