/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.ui.resources.jobs.viewJobs;


import org.apache.commons.lang3.StringUtils;
import org.apache.hive.ui.ConnectionFactory;
import org.apache.hive.ui.ConnectionSystem;
import org.apache.hive.ui.HiveUIContext;
import org.apache.hive.ui.actor.message.HiveJob;
import org.apache.hive.ui.actor.message.SQLStatementJob;
import org.apache.hive.ui.client.AsyncJobRunner;
import org.apache.hive.ui.client.AsyncJobRunnerImpl;
import org.apache.hive.ui.client.ConnectionConfig;
import org.apache.hive.ui.hdfsclient.help.HdfsApiException;
import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsApi;
import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsUtil;
import org.apache.hive.ui.resources.jobs.ModifyNotificationDelegate;
import org.apache.hive.ui.resources.jobs.ModifyNotificationInvocationHandler;
import org.apache.hive.ui.resources.jobs.atsJobs.IATSParser;
import org.apache.hive.ui.resources.savedQueries.SavedQuery;
import org.apache.hive.ui.resources.savedQueries.SavedQueryResourceManager;
import org.apache.hive.ui.utils.BadRequestFormattedException;
import org.apache.hive.ui.utils.FilePaginator;
import org.apache.hive.ui.utils.ServiceFormattedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class JobControllerImpl implements JobController, ModifyNotificationDelegate {
    private final static Logger LOG =
            LoggerFactory.getLogger(JobControllerImpl.class);

    private HiveUIHdfsApi hiveUIHdfsApi;
    private Job jobUnproxied;
    private Job job;
    private boolean modified;

    private IATSParser atsParser;

    /**
     * JobController constructor
     * Warning: Create JobControllers ONLY using JobControllerFactory!
     */
    public JobControllerImpl( Job job,
                             IATSParser atsParser,
                             HiveUIHdfsApi hiveUIHdfsApi) {
        setJobPOJO(job);
        this.atsParser = atsParser;
        try {
            this.hiveUIHdfsApi = HiveUIHdfsUtil.connectToHDFSApi();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    public String getQueryForJob() {
        FilePaginator paginator = new FilePaginator(job.getQueryFile(), hiveUIHdfsApi);
        String query;
        try {
            query = paginator.readPage(0);  //warning - reading only 0 page restricts size of query to 1MB
        } catch (IOException e) {
            throw new ServiceFormattedException("F030 Error when reading file " + job.getQueryFile(), e);
        } catch (InterruptedException e) {
            throw new ServiceFormattedException("F030 Error when reading file " + job.getQueryFile(), e);
        }
        return query;
    }

    private static final String DEFAULT_DB = "default";

    public String getJobDatabase() {
        if (job.getDataBase() != null) {
            return job.getDataBase();
        } else {
            return DEFAULT_DB;
        }
    }


    @Override
    public void submit() throws Throwable {
        String jobDatabase = getJobDatabase();
        String query = getQueryForJob();
        ConnectionSystem system = ConnectionSystem.getInstance();
        AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl( system.getOperationController(), system.getActorSystem());
        SQLStatementJob asyncJob = new SQLStatementJob(HiveJob.Type.ASYNC, getStatements(jobDatabase, query), HiveUIContext.getUsername(), job.getId(), job.getLogFile());
        asyncJobRunner.submitJob(getHiveConnectionConfig(), asyncJob, job);

    }

    private String[] getStatements(String jobDatabase, String query) {
      List<String> queries = Lists.asList("use " + jobDatabase, query.split(";"));
      List<String> cleansedQueries = FluentIterable.from(queries).transform(new Function<String, String>() {
          @Nullable
          @Override
          public String apply(@Nullable String s) {
              return s.trim();
          }
      }).filter(new Predicate<String>() {
          @Override
          public boolean apply(@Nullable String s) {
              return !StringUtils.isEmpty(s);
          }
      }).toList();
      return cleansedQueries.toArray(new String[0]);
    }


    @Override
    public void cancel() {
      ConnectionSystem system = ConnectionSystem.getInstance();
      AsyncJobRunner asyncJobRunner = new AsyncJobRunnerImpl( system.getOperationController(), system.getActorSystem());
      asyncJobRunner.cancelJob(job.getId(), HiveUIContext.getUsername());
    }

    @Override
    public void update() {
        updateJobDuration();
    }


    @Override
    public Job getJob() {
        return job;
    }

    /**
     * Use carefully. Returns unproxied bean object
     * @return unproxied bean object
     */
    @Override
    public Job getJobPOJO() {
        return jobUnproxied;
    }

    public void setJobPOJO(Job jobPOJO) {
        Job jobModifyNotificationProxy = (Job) Proxy.newProxyInstance(jobPOJO.getClass().getClassLoader(),
                new Class[]{Job.class},
                new ModifyNotificationInvocationHandler(jobPOJO, this));
        this.job = jobModifyNotificationProxy;

        this.jobUnproxied = jobPOJO;
    }


    @Override
    public void afterCreation() {
        setupStatusDirIfNotPresent();
        setupQueryFileIfNotPresent();
        setupLogFileIfNotPresent();
        setCreationDate();
    }

    public void setupLogFileIfNotPresent() {
        if (job.getLogFile() == null || job.getLogFile().isEmpty()) {
            setupLogFile();
        }
    }

    public void setupQueryFileIfNotPresent() {
        if (job.getQueryFile() == null || job.getQueryFile().isEmpty()) {
            setupQueryFile();
        }
    }

    public void setupStatusDirIfNotPresent() {
        if (job.getStatusDir() == null || job.getStatusDir().isEmpty()) {
            setupStatusDir();
        }
    }

    private static final long MillisInSecond = 1000L;

  public void updateJobDuration() {
    job.setDuration((System.currentTimeMillis() / MillisInSecond) - (job.getDateSubmitted() / MillisInSecond));
  }

  public void setCreationDate() {
    job.setDateSubmitted(System.currentTimeMillis());
  }

  private void setupLogFile() {
    LOG.debug("Creating log file for job#" + job.getId());

        String logFile = job.getStatusDir() + "/" + "logs";
        try {
            HiveUIHdfsUtil.putStringToFile(hiveUIHdfsApi, logFile, "abc");
        } catch (HdfsApiException e) {
            throw new ServiceFormattedException(e);
        }

        job.setLogFile(logFile);
        LOG.debug("Log file for job#" + job.getId() + ": " + logFile);
    }

    private void setupStatusDir() {
        String newDirPrefix = makeStatusDirectoryPrefix();
        String newDir = null;
        try {
            newDir = HiveUIHdfsUtil.findUnallocatedFileName(hiveUIHdfsApi, newDirPrefix, "");
        } catch (HdfsApiException e) {
            throw new ServiceFormattedException(e);
        }

        job.setStatusDir(newDir);
        LOG.debug("Status dir for job#" + job.getId() + ": " + newDir);
    }

    private String makeStatusDirectoryPrefix() {
        String userScriptsPath ="/user/admin/hive/jobs";  // admin

        String normalizedName = String.format("hive-job-%s", job.getId());
        String timestamp = new SimpleDateFormat("yyyy-MM-dd_hh-mm").format(new Date());
        return String.format(userScriptsPath +
                "/%s-%s", normalizedName, timestamp);
    }

    private void setupQueryFile() {
        String statusDir = job.getStatusDir();
        assert statusDir != null : "setupStatusDir() should be called first";

        String jobQueryFilePath = statusDir + "/" + "query.hql";

        try {

            if (job.getForcedContent() != null) {

               HiveUIHdfsUtil.putStringToFile(hiveUIHdfsApi, jobQueryFilePath, job.getForcedContent());
                job.setForcedContent("");  // prevent forcedContent to be written to DB

            } else if (job.getQueryId() != null) {

                String savedQueryFile = getRelatedSavedQueryFile();
                hiveUIHdfsApi.copy(savedQueryFile, jobQueryFilePath);
                job.setQueryFile(jobQueryFilePath);

            } else {

                throw new BadRequestFormattedException("queryId or forcedContent should be passed!", null);

            }

        } catch (IOException e) {
            throw new ServiceFormattedException("F040 Error when creating file " + jobQueryFilePath, e);
        } catch (InterruptedException e) {
            throw new ServiceFormattedException("F040 Error when creating file " + jobQueryFilePath, e);
        } catch (HdfsApiException e) {
            throw new ServiceFormattedException(e);
        }
        job.setQueryFile(jobQueryFilePath);

        LOG.debug("Query file for job#" + job.getId() + ": " + jobQueryFilePath);
    }


    private ConnectionConfig getHiveConnectionConfig() {
        return ConnectionFactory.create();
    }

    private String getRelatedSavedQueryFile() {
        SavedQuery savedQuery;
        savedQuery = SavedQueryResourceManager.read(job.getQueryId());
        if(savedQuery == null){
            throw new BadRequestFormattedException("queryId not found!", null);
        }
        return savedQuery.getQueryFile();
    }

    @Override
    public boolean onModification(Object object) {
        setModified(true);
        return true;
    }

    @Override
    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    @Override
    public void clearModified() {
        setModified(false);
    }
}
