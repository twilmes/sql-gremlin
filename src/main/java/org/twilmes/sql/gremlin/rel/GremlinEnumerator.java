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

/**
 * Created by twilmes on 9/25/15.
 * Modified by lyndonb-bq on 05/17/21.
 */

import org.apache.calcite.linq4j.Enumerator;
import java.util.Iterator;
import java.util.List;


/**
 * Enumerator that reads from a list result set.
 */
class GremlinEnumerator<E> implements Enumerator<E> {
    private final Iterator<Object[]> cursor;
    private E current;

    public GremlinEnumerator(final List<Object[]> rows) {
        this.cursor = rows.iterator();
    }

    public E current() {
        return current;
    }

    public boolean moveNext() {
        try {
            if (cursor.hasNext()) {
                final E row = (E) cursor.next();
                current = row;
                return true;
            } else {
                current = null;
                return false;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // nothing to close
    }
}