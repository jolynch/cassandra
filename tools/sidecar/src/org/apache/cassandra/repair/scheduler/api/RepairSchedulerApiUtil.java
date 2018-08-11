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

import spark.Filter;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;
import spark.Route;

/**
 * This class holds RepairSchedule API utilities, this is very specific to spark framework
 */
public class RepairSchedulerApiUtil
{
    /**
     * Add JSON ContentType to all responses
     */
    public static final Filter addRepairSchedulerHeaders = (Request request, Response response) -> {
        response.type("application/json");
        if (response.status() <= 0)
        {
            response.status(200);
        }
    };

    /**
     * Utility method to read Schedule parameter from spark request object.
     *
     * @param request request object to read the schedule param from
     * @return schedule name
     */
    public static String getParamSchedule(Request request)
    {
        return request.params("schedule");
    }

    /**
     * Map the route for HTTP GET requests.
     * This adds default transformers needed for RepairScheduler APIs
     *
     * @param path  the path
     * @param route The route
     */
    public static void get(String path, Route route)
    {
        spark.Spark.get(path, route, new JsonSerializer());
    }

    /**
     * Map the route for HTTP POST requests
     *
     * @param path  the path
     * @param route The route
     */
    public static void post(String path, Route route)
    {
        spark.Spark.post(path, route, new JsonSerializer());
    }

    /**
     * Responsible for serializing objects to JSON before sending over the wire to client.
     * It uses Jackson serialization library to serialize.
     * Getters and Setters are ignored in serialization to keep the deserialization is simpler
     */
    static class JsonSerializer implements ResponseTransformer
    {
        @Override
        public String render(Object data)
        {
            return RepairSchedulerJsonAdapter.serializeJson(data);
        }
    }
}