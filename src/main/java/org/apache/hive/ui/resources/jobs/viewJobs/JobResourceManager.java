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

package org.apache.hive.ui.resources.jobs.viewJobs;


import org.apache.hive.ui.persistence.service.HiveUIJobService;
import org.apache.hive.ui.persistence.po.HiveIJobEntity;
import org.apache.hive.ui.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Object that provides CRUD operations for job objects
 */
public class JobResourceManager  {
  private final static Logger LOG =
      LoggerFactory.getLogger(JobResourceManager.class);

  private IJobControllerFactory jobControllerFactory;

  /**
   * Constructor
   * @param context View Context instance
   */
  public JobResourceManager(SharedObjectsFactory sharedObjectsFactory) {
    jobControllerFactory = sharedObjectsFactory.getJobControllerFactory();
  }



  public void save(Job job){
    HiveIJobEntity newentity = new HiveIJobEntity();

    newentity.setTitle(job.getTitle());
    newentity.setQueryFile(job.getQueryFile());

    newentity.setStatusDir(job.getStatusDir());
    newentity.setDateSubmitted(job.getDateSubmitted());
    newentity.setDuration(job.getDuration());

    newentity.setForcedContent(job.getForcedContent());
    newentity.setDataBase(job.getDataBase());
    newentity.setQueryId(job.getQueryId());


    newentity.setSqlState(job.getSqlState());
    newentity.setStatusMessage(job.getStatusMessage());
    newentity.setStatus(job.getStatus());

    newentity.setApplicationId(job.getApplicationId());
    newentity.setDagName(job.getDagName());
    newentity.setDagId(job.getDagId());

    newentity.setSessionTag(job.getSessionTag());
    newentity.setReferrer(job.getReferrer());
    newentity.setGlobalSettings(job.getGlobalSettings());

    newentity.setOwner("admin");
    newentity.setUserID(1);

    newentity.setLogFile(job.getLogFile());
    newentity.setConfFile(job.getConfFile());

    newentity.setGuid(job.getGuid());
    newentity.setHiveQueryId(job.getHiveQueryId());
    Integer jobId = HiveUIJobService.saveHiveUIJob(newentity);
    if(jobId != null){
      job.setId(jobId.toString());
    }


  }

  public boolean update(Job job){
    HiveIJobEntity updated = HiveUIJobService.findJobById(Integer.parseInt(job.getId()));
    if(updated == null) return false;
    updated.setTitle(job.getTitle());
    updated.setQueryFile(job.getQueryFile());

    updated.setStatusDir(job.getStatusDir());
    updated.setDateSubmitted(job.getDateSubmitted());
    updated.setDuration(job.getDuration());

    updated.setForcedContent(job.getForcedContent());
    updated.setDataBase(job.getDataBase());
    updated.setQueryId(job.getQueryId());


    updated.setSqlState(job.getSqlState());
    updated.setStatusMessage(job.getStatusMessage());
    updated.setStatus(job.getStatus());

    updated.setApplicationId(job.getApplicationId());
    updated.setDagName(job.getDagName());
    updated.setDagId(job.getDagId());

    updated.setSessionTag(job.getSessionTag());
    updated.setReferrer(job.getReferrer());
    updated.setGlobalSettings(job.getGlobalSettings());

    updated.setOwner("admin");
    updated.setUserID(1);

    updated.setLogFile(job.getLogFile());
    updated.setConfFile(job.getConfFile());

    updated.setGuid(job.getGuid());
    updated.setHiveQueryId(job.getHiveQueryId());
    if(!HiveUIJobService.updateHiveUIJob(updated)){
      return false;
    };
    return true;
  }

  public Job read(String id)  {
    HiveIJobEntity entity = HiveUIJobService.findJobById(Integer.parseInt(id));
    JobImpl job = null;
    if(entity != null){
      job = new JobImpl();
      job.setTitle(entity.getTitle());
      job.setQueryFile(entity.getQueryFile());
      job.setStatusDir(entity.getStatusDir());
      job.setDateSubmitted(entity.getDateSubmitted());
      job.setDuration(entity.getDuration());
      job.setForcedContent(entity.getForcedContent());
      job.setDataBase(entity.getDataBase());
      job.setQueryId(entity.getQueryId());
      job.setSqlState(entity.getSqlState());
      job.setStatusMessage(entity.getStatusMessage());
      job.setStatus(entity.getStatus());
      job.setApplicationId(entity.getApplicationId());
      job.setDagName(entity.getDagName());
      job.setDagId(entity.getDagId());
      job.setSessionTag(entity.getSessionTag());
      job.setReferrer(entity.getReferrer());
      job.setGlobalSettings(entity.getGlobalSettings());
      job.setOwner("admin");
      job.setId(entity.getUserID().toString());
      job.setLogFile(entity.getLogFile());
      job.setConfFile(entity.getConfFile());
      job.setGuid(entity.getGuid());
      job.setHiveQueryId(entity.getHiveQueryId());
    }
    return (Job)job;

  }


  public List<Job> findJobsByGuiId(String guid) {
    List<HiveIJobEntity> result = HiveUIJobService.findJobsByGuiId(guid);
    List<Job> lists = new ArrayList<>();
    if(result != null){
      for(HiveIJobEntity entity: result){
        Job job = new JobImpl();
        job.setTitle(entity.getTitle());
        job.setQueryFile(entity.getQueryFile());
        job.setStatusDir(entity.getStatusDir());
        job.setDateSubmitted(entity.getDateSubmitted());
        job.setDuration(entity.getDuration());
        job.setForcedContent(entity.getForcedContent());
        job.setDataBase(entity.getDataBase());
        job.setQueryId(entity.getQueryId());
        job.setSqlState(entity.getSqlState());
        job.setStatusMessage(entity.getStatusMessage());
        job.setStatus(entity.getStatus());
        job.setApplicationId(entity.getApplicationId());
        job.setDagName(entity.getDagName());
        job.setDagId(entity.getDagId());
        job.setSessionTag(entity.getSessionTag());
        job.setReferrer(entity.getReferrer());
        job.setGlobalSettings(entity.getGlobalSettings());
        job.setOwner("admin");
        job.setId(entity.getUserID().toString());
        job.setLogFile(entity.getLogFile());
        job.setConfFile(entity.getConfFile());
        job.setGuid(entity.getGuid());
        job.setHiveQueryId(entity.getHiveQueryId());
        lists.add(job);

      }

    }
    return lists;
  }
  public List<Job> readAll() {
    List<HiveIJobEntity> result = HiveUIJobService.findJobsByUserId(1);
    List<Job> lists = new ArrayList<>();
    if(result != null){
      for(HiveIJobEntity entity: result){
        Job job = new JobImpl();
        job.setTitle(entity.getTitle());
        job.setQueryFile(entity.getQueryFile());
        job.setStatusDir(entity.getStatusDir());
        job.setDateSubmitted(entity.getDateSubmitted());
        job.setDuration(entity.getDuration());
        job.setForcedContent(entity.getForcedContent());
        job.setDataBase(entity.getDataBase());
        job.setQueryId(entity.getQueryId());
        job.setSqlState(entity.getSqlState());
        job.setStatusMessage(entity.getStatusMessage());
        job.setStatus(entity.getStatus());
        job.setApplicationId(entity.getApplicationId());
        job.setDagName(entity.getDagName());
        job.setDagId(entity.getDagId());
        job.setSessionTag(entity.getSessionTag());
        job.setReferrer(entity.getReferrer());
        job.setGlobalSettings(entity.getGlobalSettings());
        job.setOwner("admin");
        job.setId(entity.getUserID().toString());
        job.setLogFile(entity.getLogFile());
        job.setConfFile(entity.getConfFile());
        job.setGuid(entity.getGuid());
        job.setHiveQueryId(entity.getHiveQueryId());
        lists.add(job);

      }

    }
    return lists;
  }


  public boolean delete(String id) {
    HiveIJobEntity del =  HiveUIJobService.findJobById(Integer.parseInt(id));
    if(del != null){
      if(!HiveUIJobService.deleteHiveUIJob(del)){
        // handle error later.
        return false;
      }
    }
    return  true;
  }




  public Job create(Job object) {
    save(object);
    JobController jobController = jobControllerFactory.createControllerForJob(object);
    jobController.afterCreation();
    saveIfModified(jobController);
    return object;
  }

  public void saveIfModified(JobController jobController) {
    if (jobController.isModified()) {
      update(jobController.getJobPOJO());
      jobController.clearModified();
    }
  }





  public JobController readController(Object id) {
    Job job = read((String)id);
    return jobControllerFactory.createControllerForJob(job);
  }
}
