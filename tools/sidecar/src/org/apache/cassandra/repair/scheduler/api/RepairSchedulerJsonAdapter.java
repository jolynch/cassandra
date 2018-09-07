/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.repair.scheduler.api;

import java.io.IOException;
import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Json Adapter for serializing and de-serializing the Repair Metadata/ config objects to and from JSON
 * This is used in RepairSchdeulerAPI
 */
public class RepairSchedulerJsonAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(RepairSchedulerJsonAdapter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * De-serializes the JSONString to class
     *
     * @param clazz Class name to deserialize the JSON String to
     * @param body  JSON string
     * @param <T>   Class type
     * @return Instance of a class constructed from JSON string
     * @throws IOException Exception during de-serializing the JSON String
     */
    public static <T> T deserializeJson(Class<T> clazz, String body) throws IOException
    {
        return mapper.readValue(body, clazz);
    }

    /**
     * Serializes the object to JSON String
     *
     * @param data Object to serialize
     * @return JSON String
     */
    static String serializeJson(Object data)
    {
        try
        {
            StringWriter sw = new StringWriter();
            mapper.writeValue(sw, data);
            return sw.toString();
        }
        catch (IOException e)
        {
            logger.error("IOException while serializing the object.", e);
            throw new RuntimeException("IOException while serializing the object.");
        }
    }

    static
    {
        mapper.setVisibilityChecker(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                                          .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                                          .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                                          .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                                          .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    }
}
