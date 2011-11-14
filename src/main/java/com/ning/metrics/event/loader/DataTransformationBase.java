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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DataTransformationBase implements DataTransformation
{
    private final static Pattern DOT_SPLIT = Pattern.compile("\\.");
    private final static Pattern IP_ADDRESS = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
    protected static final Charset LATIN1 = Charset.forName("ISO-8859-1");
    private static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public String transformRow(List<GoodwillSchemaField> columnTypes, HashMap columnValues, String delimiter)
    {
        StringBuffer outputRow = new StringBuffer();
        for (GoodwillSchemaField column : columnTypes) {
            try {
                String colValue = columnValues.get(column.getName())==null?"":columnValues.get(column.getName()).toString();
                log.debug(column.getName().toString());
                if (column.getType().toString().equals("STRING")) {
                    outputRow.append("\"").append(colValue.replace("\"", "").replace("\'", "").replace("\\", "\\\\")).append("\"").append(delimiter);
                }
                else {
                    if (column.getType().toString().equals("DATE")) {
                        outputRow.append("\"").append(dateFormatter.print(Long.valueOf(colValue))).append("\"").append(delimiter);
                    }
                    else {
                        outputRow.append(colValue).append(delimiter);
                    }
                }
                log.debug(outputRow.toString());
            }
            catch (Exception e) {
                outputRow.append(delimiter);
                log.debug("Missing field" + column.getName().toString());
            }
        }
        return outputRow.append("\n").toString();
    }


     /**
     * Convert an IP address from a byte array to a decimal representation
     * You can use this method if you want to convert IP addresses to int for optimal netezza storage
     * @param bytes ip as bytes (dot decimal form or decimal)
     * @return decimal value of the ip address
     */
    protected static int ipToInt(String ipStr)
    {
        // No info? Convert to 0
        if (ipStr.length() == 0) {
            return 0;
        }
        // Is is a dot decimal representation?
        if (ipStr.indexOf('.') > 0) {
            String[] parts = DOT_SPLIT.split(ipStr);
            // valid ip address must have 4 octets
            if (parts.length == 4) {
                int ip = 0;
                try {
                    for (int n = 0; n < 4; n++) {
                        int part = Integer.parseInt(parts[n]);
                        ip <<= 8;
                        ip += part;
                    }
                    return ip + (1 << 31);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid IP address '" + ipStr + "': " + e.getLocalizedMessage());
                }
            }
        }
        // nope, straight 32-bit number:
        return ipStringToInt32(ipStr);
    }

    /**
     * Helper method used to convert a String that represents 32-bit integer
     * (which may signed or unsigned) into Java 32-bit signed int; but
     * verifying that there is no overflow
     * You can use this method if you want to convert IP addresses to int for optimal netezza storage
     * @param ipStr String that represent 32-bit value
     * @return 32-bit signed int parsed from the String, if valid (throws
     *         {@link IllegalArgumentException} if invalid)
     */
    private static int ipStringToInt32(String ipStr)
    {
        try {
            if (ipStr.startsWith("-")) { // negative; must be within 32-bit int range
                return Integer.parseInt(ipStr);
            }
            // positive; need to parse as Long
            long l = Long.parseLong(ipStr);
            // but must not overflow
            if (l > 0x0FFFFFFFFl) { // throw, will be caught, rethrown below:
                throw new NumberFormatException("Overflow, value " + l + " outside of uint32 value range");
            }
            return (int) l+ (1 << 31);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid IP address '" + ipStr + "': " + e.getLocalizedMessage());
        }
    }

}
