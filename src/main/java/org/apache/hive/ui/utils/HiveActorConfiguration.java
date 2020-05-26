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

package org.apache.hive.ui.utils;

/**
 * This fetches the configuration for the actor system from ambari.properties
 */
public class HiveActorConfiguration {
  private static String DEFAULT_CONFIG = "default";
  private static String CONNECTION_INACTIVITY_TIMEOUT = "hive.ui.connection.inactivity.timeout";
  private static String CONNECTION_TERMINATION_TIMEOUT = "hive.ui.connection.termination.timeout";
  private static String SYNC_QUERY_TIMEOUT = "hive.ui.sync.query.timeout";
  private static String RESULT_FETCH_TIMEOUT = "hive.ui.result.fetch.timeout";


  public HiveActorConfiguration() {

  }

  private String getPropertiesFromDB(String key,String defaultValue){
    // hardcode here
   return "7200000";
  }

  public long getInactivityTimeout(long defaultValue) {
    return Long.parseLong(getPropertiesFromDB(CONNECTION_INACTIVITY_TIMEOUT, String.valueOf(defaultValue)));
  }

  public long getTerminationTimeout(long defaultValue) {
    return Long.parseLong(getPropertiesFromDB(CONNECTION_TERMINATION_TIMEOUT, String.valueOf(defaultValue)));
  }

  public long getSyncQueryTimeout(long defaultValue) {
    return Long.parseLong(getPropertiesFromDB(SYNC_QUERY_TIMEOUT, String.valueOf(defaultValue)));
  }

  public long getResultFetchTimeout(long defaultValue) {
    return Long.parseLong(getPropertiesFromDB(RESULT_FETCH_TIMEOUT, String.valueOf(defaultValue)));
  }


}
