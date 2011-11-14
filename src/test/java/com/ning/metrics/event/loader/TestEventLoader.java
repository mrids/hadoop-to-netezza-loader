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

import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestEventLoader
{

    private List<String> allEventFields;
    private EventLoaderConfiguration configuration = new EventLoaderConfiguration();
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test(groups = "integration")
    @BeforeClass(alwaysRun = true)
    public void setUpGlobal() throws Exception
    {
        configuration.setPath("test");
        configuration.setHost("localhost");
        configuration.setPort("8080");
        configuration.setRaw(false);
        configuration.setGoodwill("goodwill.ningops.com");

    }

    @Test(enabled = false, groups = "integration")
    public void testBazelCSPerfLiteEventLoad() throws Exception
    {

        String eventRow = readFile("BazelCSPerflite.txt");
        String eventName = "BazelCSPerfLite";
        System.setProperty("default", "com.ning.metrics.event.loader.DataTransformationEvents");
         configuration.setEventType(eventName);
         configuration.setOutputFile("BazelCSPerfLite.txt");
         EventLoader eventLoader = new EventLoader(configuration);
         SchemaLookup schemaLookup = new SchemaLookup(eventName, eventLoader.goodwill, null);
          List<GoodwillSchemaField> goodwillSchemaFields = schemaLookup.getFields();
          ArrayList<String> columnNames = schemaLookup.getColumns();
         JsonFactory jsonFactory = new JsonFactory();
         JsonParser jsonParser = jsonFactory.createJsonParser(new File("BazelCSPerfLite.txt"));
         eventLoader.getCSVFormat(jsonParser, columnNames, goodwillSchemaFields);
         System.out.println(readFile("BazelCSPerfLite.txt.csv"));
        // Assert.assertTrue(readFile("BazelCSPerfLite.txt.csv").equals(expectedOutput));
     }

    @Test(enabled = false ,groups = "integration")
    public void testFrontDoorVisitEventLoad() throws Exception
    {
        String expectedOutput = "2011-06-24 23:20:22.623|\"/icons/appatar/2144723?default=2144723&width=44&height=44\"|\"http://www.xnq1.ningops.net/plans\"|IE|8|0|WinXP|1|0|\"5d4eeb81-ce81-4306-ba41-daa61124977c\"|0|0|\"\"|\"api.xnq1.ningops.net\"|\"image/jpeg\"|-1060064914|US|38|-97|200|1702|2843|\"GET\"|\"http\"|2842|\"10.18.43.115,(10.18.43.80,200,2840)\"|\"13b03aad-e1c0-434e-b74f-1640eb56f4c8\"|\"10.15.12.19, 10.18.240.62\"|\"7243d767-0a5e-4b7b-a055-b07e5eb2a756\"|";
        String eventName = "FrontDoorVisit";
        System.setProperty(eventName, "com.ning.metrics.event.loader.DataTransformationFrontDoorVisit");
        System.setProperty("default", "com.ning.metrics.event.loader.DataTransformationEvents");
        configuration.setEventType(eventName);
        configuration.setOutputFile("FrontDoorVisit.txt");
        EventLoader eventLoader = new EventLoader(configuration);
        SchemaLookup schemaLookup = new SchemaLookup(eventName, eventLoader.goodwill, null);
         List<GoodwillSchemaField> goodwillSchemaFields = schemaLookup.getFields();
         ArrayList<String> columnNames = schemaLookup.getColumns();
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createJsonParser(new File("FrontDoorVisit.txt"));
        eventLoader.getCSVFormat(jsonParser, columnNames, goodwillSchemaFields);
        Assert.assertTrue(readFile("FrontDoorVisit.txt.csv").equals(expectedOutput));
    }

    private String readFile(String inputFile) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(new File(inputFile)));
        StringBuffer eventRow = new StringBuffer();
        String line;
        while ((line = reader.readLine()) != null) {
            eventRow.append(line);
        }
        return eventRow.toString();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownGlobal() throws Exception
    {
    }
}
