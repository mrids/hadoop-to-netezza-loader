/*
 *
 *  * Copyright 2010-2011 Ning, Inc.
 *  *
 *  * Ning licenses this file to you under the Apache License, version 2.0
 *  * (the "License"); you may not use this file except in compliance with the
 *  * License.  You may obtain a copy of the License at:
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 *  * License for the specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.ning.metrics.event.loader;

import com.ning.metrics.goodwill.access.GoodwillAccessor;
import com.ning.metrics.goodwill.access.GoodwillSchema;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;

/*
  Lookup goodwill for the eventType to get the schema.

*/
public class SchemaLookup
{
    private final String eventName;
    private final int maxId;
    public final List<GoodwillSchemaField> fields;
    private final String goodwill_host;
    private static final int GOODWILL_PORT = 8080;
    private static final Logger log = Logger.getLogger(EventLoader.class);


    // class is used if you want to feed a new custom schema or just an event to lookup from goodwill.
    public SchemaLookup(String eventName, String goodwill_host, List<String> inputFields)
    {
        this.eventName = eventName;
        this.goodwill_host =goodwill_host;
        GoodwillSchema jsonEventType = getRegisteredSchema(eventName);

        // did we find the type
        if (jsonEventType == null) {
            throw new IllegalStateException("Unknown event " + this.eventName);

        }

        // The list of fields is ordered
        List<GoodwillSchemaField> schema = jsonEventType.getSchema();
        this.maxId = schema.get(schema.size() - 1).getId();

        // Determine the output schema
        if (inputFields == null || inputFields.isEmpty()) {
            this.fields = Collections.unmodifiableList(jsonEventType.getSchema());
        }
        else {
            List<GoodwillSchemaField> outputFields = new ArrayList<GoodwillSchemaField>(inputFields.size());

            for (String fieldName : inputFields) {
                GoodwillSchemaField field = jsonEventType.getFieldByName(fieldName);
                if (field == null) {
                    throw new IllegalArgumentException(eventName + " does not contain a field " + fieldName);
                }
                outputFields.add(field);
            }

            this.fields = Collections.unmodifiableList(outputFields);
        }
    }

    public ArrayList<String> getColumns()
    {
        ArrayList<String> eventFields = new ArrayList<String>();
        for (GoodwillSchemaField str : fields) {
            log.debug(str.getName());
            eventFields.add(str.getName());
        }
        return eventFields;
    }

    public String getEventName()
    {
        return eventName;
    }

    public List<GoodwillSchemaField> getFields()
    {
        return fields;
    }

    public int getMaxId()
    {
        return maxId;
    }

    public int size()
    {
        return fields.size();
    }

    /**
     * Gets a schema from the type registrar or null if unknown.  The field
     * list is sorted by fieldId.
     *
     * @param typeName Schema name to lookup
     * @return Goodwill schema associate to the name
     */
    protected GoodwillSchema getRegisteredSchema(String typeName)
    {
        GoodwillAccessor typeRegistrar = new GoodwillAccessor(goodwill_host, GOODWILL_PORT);

        for (int i = 0; i < 5; i++) {
            try {
                Future<GoodwillSchema> typeFuture = typeRegistrar.getSchema(typeName);
                if (typeFuture != null) {
                    return typeFuture.get();
                }
            }
            catch (ExecutionException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        return null;
    }
}