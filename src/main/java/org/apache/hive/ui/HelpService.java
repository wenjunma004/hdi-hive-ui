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

package org.apache.hive.ui;


import org.apache.hive.ui.persistence.po.HiveUITestEntity;
import org.apache.hive.ui.persistence.service.HiveUITestService;
import org.apache.hive.ui.resources.files.FileService;
import org.apache.hive.ui.resources.jobs.atsJobs.ATSParserFactory;
import org.apache.hive.ui.resources.jobs.atsJobs.ATSRequestsDelegateImpl;
import org.json.simple.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Help service
 */
@Path("/system")
public class HelpService extends BaseService {



  /**
   * Constructor
   */
  public HelpService() {
    super();
  }

  /**
   * Version
   * @return version
   */
  @GET
  @Path("/version")
  @Produces(MediaType.TEXT_PLAIN)
  public Response version(){
    return Response.ok("0.0.1-SNAPSHOT").build();
  }

  // ================================================================================
  // Smoke tests
  // ================================================================================

  /**
   * HDFS Status
   * @return status
   */
  @GET
  @Path("/hdfsStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response hdfsStatus(){
    FileService.hdfsSmokeTest();
    return getOKResponse();
  }

  /**
   * HomeDirectory Status
   * @return status
   */
  @GET
  @Path("/userhomeStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response userhomeStatus (){
    FileService.userhomeSmokeTest();
    return getOKResponse();
  }


  /**
   * ATS Status
   * @return status
   */
  @GET
  @Path("/atsStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response atsStatus() {
    try {
      ATSRequestsDelegateImpl atsimpl = new ATSRequestsDelegateImpl(new ATSParserFactory().getATSUrl());
      atsimpl.checkATSStatus();
      return getOKResponse();
    }catch (IOException e){
      throw new WebApplicationException(e);
    }
  }

  /**
   * Hive Status
   * @return status
   */
  @GET
  @Path("/connect")
  @Produces(MediaType.APPLICATION_JSON)
  public Response connect() {
    String HiveDriverName = "org.apache.hive.jdbc.HiveDriver";
    try {
      Class.forName(HiveDriverName);
      Connection con = DriverManager.getConnection(HiveUIContext.getHiveJDBCUrl());
      System.out.println("\nGot Connection: " + con);
      Statement stmt = con.createStatement();
      String sql = "show tables";
      System.out.println("\nExecuting Query: " + sql);
      ResultSet rs = stmt.executeQuery(sql);
      System.out.println("\n-----------------Result start------------------");
      while (rs.next()) {
        System.out.println(rs.getString(1));
      }
      System.out.println("\n-----------------Result end--------------------");
      return getOKResponse();
    }catch (Exception  e){
      throw new WebApplicationException(e);
    }
  }






  private Response getOKResponse() {
    JSONObject response = new JSONObject();
    response.put("message", "OK");
    response.put("trace", null);
    response.put("status", "200");
    return Response.ok().entity(response).type(MediaType.APPLICATION_JSON).build();
  }

  /**
   * Version
   * @return version
   */
  @GET
  @Path("/test")
  @Produces(MediaType.TEXT_PLAIN)
  public Response testStorage(){

    HiveUITestEntity test2 = new HiveUITestEntity();
    test2.setContent("test");
    boolean res = HiveUITestService.test(test2);
    if(res){
      System.out.println("test successfully ");
    }else{
      System.out.println("test fail ");
    }
    return Response.ok("OK").build();
  }
}


