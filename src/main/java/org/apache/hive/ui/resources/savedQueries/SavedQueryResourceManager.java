/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.ui.resources.savedQueries;


import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsUtil;
import org.apache.hive.ui.hdfsclient.help.HdfsApiException;
import org.apache.hive.ui.persistence.po.HiveUISaveQueryEntity;
import org.apache.hive.ui.persistence.service.HiveUISaveQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Object that provides CRUD operations for query objects
 */
public class SavedQueryResourceManager {
  private final static Logger LOG =
      LoggerFactory.getLogger(SavedQueryResourceManager.class);


  public static SavedQuery create(SavedQuery object) {
    String query = object.getShortQuery();
    object.setShortQuery(makeShortQuery(query));
    createDefaultQueryFile(object, query);
    HiveUISaveQueryEntity newObject = new HiveUISaveQueryEntity();
    newObject.setOwner("admin");
    newObject.setUserID(1);
    newObject.setTitle(object.getTitle());
    newObject.setDataBase(object.getDataBase());
    newObject.setShortQuery(object.getShortQuery());
    newObject.setQueryFile(object.getQueryFile());
    Integer id = HiveUISaveQueryService.saveHiveUISaveQuery(newObject);
    if (id != null){
        object.setId(id.toString());
        return object;
    }
    return null;
  }

  public static void createDefaultQueryFile(SavedQuery object, String query) {
//    String userScriptsPath = context.getProperties().get("scripts.dir");
    String userScriptsPath = "/user/admin/hive/scripts";


    String normalizedName = String.format("hive-query-%s", object.getId());
    String timestamp = new SimpleDateFormat("yyyy-MM-dd_hh-mm").format(new Date());
    String baseFileName = String.format(userScriptsPath +
        "/%s-%s", normalizedName, timestamp);

    String newFilePath = null;
    try {
      newFilePath = HiveUIHdfsUtil.findUnallocatedFileName(HiveUIHdfsUtil.connectToHDFSApi(), baseFileName, ".hql");
      HiveUIHdfsUtil.putStringToFile(HiveUIHdfsUtil.connectToHDFSApi(), newFilePath, query);
    } catch (HdfsApiException e) {
      e.printStackTrace();
    }

    object.setQueryFile(newFilePath);
  }


  public static SavedQuery read(String id)  {
    HiveUISaveQueryEntity saved =  HiveUISaveQueryService.findSaveQueryById(Integer.parseInt(id));
    if(saved != null){
      SavedQuery savedQuery = new SavedQuery();
      savedQuery.setOwner(saved.getOwner());
      savedQuery.setTitle(saved.getTitle());
      savedQuery.setDataBase(saved.getDataBase());
      savedQuery.setShortQuery(saved.getShortQuery());
      savedQuery.setQueryFile(saved.getQueryFile());
      savedQuery.setId(saved.getId().toString());
      return savedQuery;
    }
    return null;
  }



  /**
   * Generate short preview of query.
   * Remove SET settings like "set hive.execution.engine=tez;" from beginning
   * and trim to 42 symbols.
   * @param query full query
   * @return shortened query
   */
  protected static String makeShortQuery(String query) {
    query = query.replaceAll("(?i)set\\s+[\\w\\-.]+(\\s*)=(\\s*)[\\w\\-.]+(\\s*);", "");
    query = query.trim();
    return query.substring(0, (query.length() > 42) ? 42 : query.length());
  }


  public static SavedQuery update(SavedQuery object, String id)  {
    HiveUISaveQueryEntity updated =  HiveUISaveQueryService.findSaveQueryById(Integer.parseInt(id));
    String query = object.getShortQuery();
    object.setShortQuery(makeShortQuery(query));
    if(updated != null){
      createDefaultQueryFile(object, query);
      updated.setOwner("admin");
      updated.setUserID(1);
      updated.setTitle(object.getTitle());
      updated.setDataBase(object.getDataBase());
      updated.setShortQuery(object.getShortQuery());
      updated.setQueryFile(object.getQueryFile());
      HiveUISaveQueryService.updateHiveUISaveQuery(updated);
      return object;

    }
    return null;
  }


  public static  List<SavedQuery> readAll() {
    List<HiveUISaveQueryEntity> list = HiveUISaveQueryService.findSaveQueriesByUserId(1);
    List<SavedQuery> items = new ArrayList<>();
    if(list != null){
      for(HiveUISaveQueryEntity entity: list){
        SavedQuery savedQuery = new SavedQuery();
        savedQuery.setOwner(entity.getOwner());
        savedQuery.setTitle(entity.getTitle());
        savedQuery.setDataBase(entity.getDataBase());
        savedQuery.setShortQuery(entity.getShortQuery());
        savedQuery.setQueryFile(entity.getQueryFile());
        savedQuery.setId(entity.getId().toString());
        items.add(savedQuery);
      }
    }
    return items;
  }


  public static void delete(String id)  {
    HiveUISaveQueryEntity del = HiveUISaveQueryService.findSaveQueryById(Integer.parseInt(id));
    if(del != null){
      HiveUISaveQueryService.deleteHiveUISaveQuery(del);
    }
  }
}
