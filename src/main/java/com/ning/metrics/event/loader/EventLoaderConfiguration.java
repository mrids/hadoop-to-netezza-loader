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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

/**
 * Configuration class that contains definitions of all configurable
 * settings to control event export.
 */
public class EventLoaderConfiguration
{
    private static Logger log = Logger.getLogger(EventLoaderConfiguration.class);

    public static enum OutputFormat
    {
        JSON, NZPIPE
    }

    private static final Options options = new Options();

    static {
        options.addOption("h", "help", false, "print this message");
        options.addOption("v", "verbose", false, "output progress to standard error");
        options.addOption("r", "raw", false, "get raw json?");
        options.addOption("n", "skipjsondownload", false, "skip download of json file?");
        //noinspection AccessStaticViaInstance
        options.addOption(OptionBuilder.withLongOpt("useragent")
            .withDescription("decode user agent into browser, version and platform")
            .hasArg()
            .withArgName("browscap.ini")
            .create('u'));

        options.addOption(OptionBuilder.withLongOpt("fields")
            .withDescription("select fields to output")
            .hasArg()
            .withArgName("fields")
            .create('f'));
        options.addOption(OptionBuilder.withLongOpt("host")
            .withDescription("action core host")
            .hasArg()
            .withArgName("host")
            .create("s"));
        options.addOption(OptionBuilder.withLongOpt("port")
            .withDescription("port number for the action core server")
            .hasArg()
            .withArgName("port")
            .create("i"));
        options.addOption(OptionBuilder.withLongOpt("path")
            .withDescription("the path in hadoop for the event data")
            .hasArg()
            .withArgName("path")
            .create("p"));

        options.addOption(OptionBuilder.withLongOpt("event")
            .withDescription("the event type to load")
            .hasArg()
            .withArgName("event")
            .create("e"));

        options.addOption(OptionBuilder.withLongOpt("geoip")
            .withDescription("add latitude, longitude and country code for every IP address")
            .hasArg()
            .withArgName("geo-ip-db")
            .create('g'));

        options.addOption(OptionBuilder.withLongOpt("goodwill")
            .withDescription("goodwill schema accessor")
            .hasArg()
            .withArgName("goodwill")
            .create('d'));

         options.addOption(OptionBuilder.withLongOpt("outputfile")
            .withDescription("output file " + Arrays.toString(OutputFormat.values()))
            .hasArg()
            .withArgName("outputfile")
            .create('o'));
    }

    private String eventType;
    private String geoIpDb;
    private boolean verbose;
    private boolean raw = false;
    private boolean skipJson =false;
    private String host = "localhost";
    private String path = "/events/xnq1/";
    private int port = 8080;
    private String outputFile;
    private List<String> fields = new ArrayList<String>();
    private String browsecapIni;
    private OutputFormat outputFormat = OutputFormat.JSON;
    private String goodwill ="goodwill.ningops.com";
    private String configurationFile = "schematransformation.properties";


    public static EventLoaderConfiguration parseArguments(String... args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption('h')) {
            return null;
        }


        EventLoaderConfiguration configuration = new EventLoaderConfiguration();
        if (line.hasOption('v')) {
            configuration.setVerbose(true);
        }
        if (line.hasOption('r')) {
            configuration.setRaw(true);
        }
        String host = line.getOptionValue("host");
        if (host != null) {
            configuration.setHost(host);
        }
        String port = line.getOptionValue("port");
        if (port == null) {
            configuration.setPort(port);
        }
        String path = line.getOptionValue("path");
        if (path != null) {
            configuration.setPath(path);
        }

        String event = line.getOptionValue("event");
        if (event != null) {
            configuration.setEventType(event);
        }

        String geoIpDb = line.getOptionValue("geoip");
        if (geoIpDb != null) {
            configuration.setGeoIpDb(geoIpDb);
        }

        String outputFile = line.getOptionValue("outputfile");
        try {
            configuration.setOutputFile(outputFile);
        }
        catch (IllegalArgumentException e) {
            return null;
        }

        String browsecapIni = line.getOptionValue("useragent");
        if (browsecapIni != null) {
            configuration.setBrowsecapIni(browsecapIni);
        }

        String outputFields = line.getOptionValue("fields");
        if (outputFields != null) {
            String[] fields = outputFields.split(",");
            configuration.getFields().addAll(Arrays.asList(fields));
        }

        String outputFormatString = line.getOptionValue("outputformat");
        if (outputFormatString != null) {
            try {
                OutputFormat outputFormat = OutputFormat.valueOf(outputFormatString);
                configuration.setOutputFormat(outputFormat);
            }
            catch (IllegalArgumentException e) {
                return null;
            }
        }

        if (line.hasOption('d')){
            configuration.setGoodwill(line.getOptionValue('d'));
        }

        if (line.hasOption('n')){
            configuration.setSkipDownloadFlag(true);
        }
        return configuration;
    }

    public static void usage()
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setArgName("PATH");
        log.info("eventExport [OPTION]... EVENT_TYPE PATH...," + options);
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // Accessors, mutators
   ///////////////////////////////////////////////////////////////////////
    */

    public String getEventType()
    {
        return eventType;
    }

    public void setEventType(String eventType)
    {
        this.eventType = eventType;
    }

    public String getOutputFile()
    {
        return outputFile;
    }

    public void setOutputFile(String outputFile)
    {
        this.outputFile = (outputFile == null ? getEventType() + ".csv" : outputFile);
    }

    public void setFields(List<String> outputFields)
    {
        this.fields = outputFields;
    }

    public List<String> getFields()
    {
        return fields;
    }

    public String getGeoIpDb()
    {
        return geoIpDb;
    }

    public void setGeoIpDb(String geoIpDb)
    {
        this.geoIpDb = geoIpDb;
    }

    public String getBrowsecapIni()
    {
        return browsecapIni;
    }

    public void setBrowsecapIni(String browsecapIni)
    {
        this.browsecapIni = browsecapIni;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    public void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    public boolean getRaw()
    {
        return raw;
    }

    public void setRaw(boolean raw)
    {
        this.raw = raw;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public String getHost()
    {
        return host;
    }

    public void setPort(String port)
    {
        this.port = Integer.parseInt(port);
    }

    public int getPort()
    {
        return port;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getPath()
    {
        return path;
    }

    public String getGoodwill()
    {
        return goodwill;
    }

    public void setGoodwill(String goodwill)
    {
        this.goodwill = goodwill;
    }

    public OutputFormat getOutputFormat()
    {
        return outputFormat;
    }

    public void setOutputFormat(OutputFormat outputFormat)
    {
        this.outputFormat = outputFormat;
    }

    public boolean getSkipDownloadFlag()
    {
        return skipJson;
    }

    public void setSkipDownloadFlag(boolean skipJson)
    {
        this.skipJson = skipJson;
    }}