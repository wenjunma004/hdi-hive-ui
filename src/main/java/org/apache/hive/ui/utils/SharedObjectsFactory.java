/**
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

import org.apache.hive.ui.HiveUIContext;
import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsApi;
import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsUtil;
import org.apache.hive.ui.resources.jobs.atsJobs.ATSParser;
import org.apache.hive.ui.resources.jobs.atsJobs.ATSParserFactory;
import org.apache.hive.ui.resources.jobs.rm.RMParser;
import org.apache.hive.ui.resources.jobs.rm.RMParserFactory;
import org.apache.hive.ui.resources.jobs.viewJobs.IJobControllerFactory;
import org.apache.hive.ui.resources.jobs.viewJobs.JobControllerFactory;
import org.apache.hive.ui.hdfsclient.help.HdfsApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generates shared connections. Clients with same tag will get the same connection.
 * e.g. user 'admin' using view instance 'HIVE1' will use one connection, another user
 * will use different connection.
 */
public class SharedObjectsFactory {
  public static final String VIEW_CONF_KEYVALUES = "view.conf.keyvalues";

  protected final static Logger LOG =
      LoggerFactory.getLogger(SharedObjectsFactory.class);

  private final ATSParserFactory atsParserFactory;
  private final RMParserFactory rmParserFactory;

  private static final Map<Class, Map<String, Object>> localObjects = new ConcurrentHashMap<Class, Map<String, Object>>();

  public SharedObjectsFactory() {
    this.atsParserFactory = new ATSParserFactory();
    this.rmParserFactory = new RMParserFactory();

    synchronized (localObjects) {
      if (localObjects.size() == 0) {
        //localObjects.put(OperationHandleControllerFactory.class, new ConcurrentHashMap<String, Object>());
       // localObjects.put(Storage.class, new ConcurrentHashMap<String, Object>());
        localObjects.put(IJobControllerFactory.class, new ConcurrentHashMap<String, Object>());
        localObjects.put(ATSParser.class, new ConcurrentHashMap<String, Object>());
//        localObjects.put(SavedQueryResourceManager.class, new ConcurrentHashMap<String, Object>());
        localObjects.put(HiveUIHdfsApi.class, new ConcurrentHashMap<String, Object>());
        localObjects.put(RMParser.class, new ConcurrentHashMap<String, Object>());
      }
    }
  }

  // =============================

  /*public OperationHandleControllerFactory getOperationHandleControllerFactory() {
    if (!localObjects.get(OperationHandleControllerFactory.class).containsKey(getTagName()))
      localObjects.get(OperationHandleControllerFactory.class).put(getTagName(), new OperationHandleControllerFactory(context, this));
    return (OperationHandleControllerFactory) localObjects.get(OperationHandleControllerFactory.class).get(getTagName());
  }*/

  // =============================


  // =============================
  public IJobControllerFactory getJobControllerFactory() {
    if (!localObjects.get(IJobControllerFactory.class).containsKey(getTagName()))
      localObjects.get(IJobControllerFactory.class).put(getTagName(), new JobControllerFactory( this));
    return (IJobControllerFactory) localObjects.get(IJobControllerFactory.class).get(getTagName());
  }

  // =============================

//  public SavedQueryResourceManager getSavedQueryResourceManager() {
//    if (!localObjects.get(SavedQueryResourceManager.class).containsKey(getTagName()))
//      localObjects.get(SavedQueryResourceManager.class).put(getTagName(), new SavedQueryResourceManager(context, this));
//    return (SavedQueryResourceManager) localObjects.get(SavedQueryResourceManager.class).get(getTagName());
//  }

  // =============================
  public ATSParser getATSParser() {
    if (!localObjects.get(ATSParser.class).containsKey(getTagName()))
      localObjects.get(ATSParser.class).put(getTagName(), atsParserFactory.getATSParser());
    return (ATSParser) localObjects.get(ATSParser.class).get(getTagName());
  }

  // =============================
  public RMParser getRMParser() {
    if (!localObjects.get(RMParser.class).containsKey(getTagName()))
      localObjects.get(RMParser.class).put(getTagName(), rmParserFactory.getRMParser());
    return (RMParser) localObjects.get(RMParser.class).get(getTagName());
  }

  // =============================
  public HiveUIHdfsApi getHdfsApi() {
    if (!localObjects.get(HiveUIHdfsApi.class).containsKey(getTagName())) {
      try {
        HiveUIHdfsApi api;
        api = HiveUIHdfsUtil.connectToHDFSApi();
        localObjects.get(HiveUIHdfsApi.class).put(getTagName(), api);
      } catch (HdfsApiException e) {
        String message = "F060 Couldn't open connection to HDFS";
        LOG.error(message);
        throw new ServiceFormattedException(message, e);
      }
    }
    return (HiveUIHdfsApi) localObjects.get(HiveUIHdfsApi.class).get(getTagName());
  }

  /**
   * Generates tag name. Clients with same tag will share one connection.
   * @return tag name
   */
  public String getTagName() {
    return String.format("%s:%s", HiveUIContext.getInstanceName(), HiveUIContext.getUsername());
  }

  /**
   * For testing purposes, ability to substitute some local object
   */
  public void setInstance(Class clazz, Object object) {
    localObjects.get(clazz).put(getTagName(), object);
  }

  /**
   * For testing purposes, ability to clear all local objects of particular class
   */
  public void clear(Class clazz) {
    localObjects.get(clazz).clear();
  }

  /**
   * For testing purposes, ability to clear all connections
   */
  public void clear() {
    for(Map<String, Object> map : localObjects.values()) {
      map.clear();
    }
  }

  /**
   *
   * Drops all objects for give instance name.
   *
   * @param instanceName
   */
  public static void dropInstanceCache(String instanceName){
    for(Map<String,Object> cache : localObjects.values()){
      for(Iterator<Map.Entry<String, Object>> it = cache.entrySet().iterator(); it.hasNext();){
        Map.Entry<String, Object> entry = it.next();
        if(entry.getKey().startsWith(instanceName+":")){
          it.remove();
        }
      }
    }
  }
}
