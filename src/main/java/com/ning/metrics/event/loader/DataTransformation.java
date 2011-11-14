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
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;

public interface    DataTransformation
{
    public final static int GEOIP_MEMORY_CACHE = 1;
    public final static int GEOIP_CHECK_CACHE = 2;

    public static Logger log = Logger.getLogger(DataTransformation.class);

    public String transformRow(List<GoodwillSchemaField> columnTypes, HashMap colValue, String delimiter);
}
   