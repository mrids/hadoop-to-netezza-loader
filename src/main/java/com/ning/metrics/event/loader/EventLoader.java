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


import com.ning.metrics.action.access.ActionAccessor;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;


public class EventLoader
{
    private static final Logger log = Logger.getLogger(EventLoader.class);
    private static final String QUERY_PARAM = "/rest/1.0/json?path=";
    private final String eventType;
    private final String path;
    private final String host;
    private final int port;
    private final boolean getRawFormat;
    private final String DELIMITER = "|";
    private final String outputFile;
    protected final String goodwill;
    protected final String recursive="&recursive=true";
    protected final boolean skipJson;

    public EventLoader(EventLoaderConfiguration configuration)
    {
        this.eventType = configuration.getEventType();
        this.path = configuration.getPath();
        this.host = configuration.getHost();
        this.port = configuration.getPort();
        this.getRawFormat = configuration.getRaw();
        this.outputFile = configuration.getOutputFile();
        this.goodwill = configuration.getGoodwill();
        this.skipJson = configuration.getSkipDownloadFlag();
        if (System.getProperty("default") == null) {
            System.setProperty("default", "com.ning.metrics.event.loader.DataTransformationBase");
        }
    }

    private void fetchEventData() throws Exception
    {
         //get the goodwill schema for the event
        SchemaLookup schemaLookup = new SchemaLookup(eventType, goodwill, null);
        List<GoodwillSchemaField> goodwillSchemaFields = schemaLookup.getFields();
        ArrayList<String> columnNames = schemaLookup.getColumns();

        // JsonParser does not support partial feeding async chunks, need to download files
        File jsonFile = new File(outputFile);
        if (!skipJson)
        {
           final FileOutputStream stream = new FileOutputStream(jsonFile);
           ActionAccessor actionAccessor = new ActionAccessor(host, port);
           actionAccessor.getPath(path, recursive.equals("true"), getRawFormat, jsonFile);
           stream.close();
        }
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createJsonParser(jsonFile);
        getCSVFormat(jsonParser, columnNames, goodwillSchemaFields);
    }

    public void getCSVFormat(JsonParser jsonParser, ArrayList<String> columnNames, List<GoodwillSchemaField> goodwillSchemaFields) throws IOException
    {
        int colNo;
        int batchCnt = 0;
        int i=0;
        BufferedOutputStream fs = new BufferedOutputStream(new FileOutputStream(outputFile + ".csv"));
        LinkedHashMap columnValues = new LinkedHashMap();
        String dataTransformationEvent = System.getProperty(eventType) == null ? System.getProperty("default") : System.getProperty(eventType);
        try {
            Class DataTransformation = Class.forName(dataTransformationEvent);
            Class[] argTypes = new Class[]{List.class, HashMap.class, String.class};
            Object dataTransformation = DataTransformation.newInstance();
            Method validateMethod = DataTransformation.getMethod("transformRow", argTypes);
            jsonParser.nextToken();
            //this is the end of the entries object
            while (!(jsonParser.nextToken() == JsonToken.END_ARRAY && jsonParser.nextToken() == JsonToken.END_OBJECT))

            {
                //check if the isDir is true in which case the content is empty
                if (jsonParser.getText().equals("isDir")) {
                    jsonParser.nextToken();
                    if (jsonParser.getText().equals("true")) {
                        while (!jsonParser.nextToken().equals(JsonToken.END_OBJECT)) {
                            ;
                        }
                    }
                }
                //if in a valid content tag then parse each tag and run transformations
                if (jsonParser.getText().equals("content")) {
                    //go with this content tag till you hit the end of array
                    while (!(jsonParser.nextToken() == JsonToken.END_ARRAY)) {
                        if (columnNames.contains(jsonParser.getText())) {
                            colNo = columnNames.indexOf(jsonParser.getText());
                            jsonParser.nextToken();
                            log.debug(columnNames.get(colNo) + "  " + jsonParser.getText());
                            columnValues.put(columnNames.get(colNo), (jsonParser.getText()));
                            i++;
                            //if you hit the end of object, its the next row,  transform and write this one first
                            if (i == columnNames.size()) {
                                Object[] inputParams = {goodwillSchemaFields, columnValues, DELIMITER};
                                fs.write(validateMethod.invoke(dataTransformation, inputParams).toString().getBytes());
                                batchCnt++;
                                if (batchCnt % 500 == 0) {
                                    log.info("done " + batchCnt);
                                    fs.flush();
                                }
                                columnValues.clear();
                                i = 0;
                            }
                        }
                    }
                }
            }

            fs.flush();
            fs.close();
                   }
            catch (Throwable e) {
            log.error("Unhandled Exception: " + e.getLocalizedMessage());
            e.printStackTrace(System.err);
            log.error("Export will fail!");
            System.exit(99);
        }
        finally {
            fs.flush();
            fs.close();
        }

    }

    public static void main(String args[]) throws Exception
    {
        log.info("Starting event export: reading configuration");
        EventLoaderConfiguration configuration = EventLoaderConfiguration.parseArguments(args);
        if (configuration == null) {
            EventLoaderConfiguration.usage();
            System.exit(1);
            return;
        }
        log.info("Starting event export: configuration read; event type:'" + configuration.getEventType() + "'; output format: "
            + configuration.getOutputFormat());
        EventLoader eventLoader = new EventLoader(configuration);
        eventLoader.fetchEventData();
        System.exit(0);
    }
}