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

package org.apache.hive.ui.resources.savedQueries;

import org.apache.hive.ui.BaseService;
import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsApi;
import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsUtil;
import org.apache.hive.ui.utils.ServiceFormattedException;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

/**
 * Servlet for queries
 * API:
 * GET /:id
 *      read SavedQuery
 * POST /
 *      create new SavedQuery
 *      Required: title, queryFile
 * GET /
 *      get all SavedQueries of current user
 */
@Path("/savedQueries")
public class SavedQueryService extends BaseService {

  protected final Logger LOG =
          LoggerFactory.getLogger(getClass());
  /**
   * Get single item
   */
  @GET
  @Path("{queryId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOne(@PathParam("queryId") String queryId,
             @QueryParam("op") String operation) {
    try {
      final SavedQuery savedQuery = SavedQueryResourceManager.read(queryId);
      if (operation != null && operation.equals("download")) {
        StreamingOutput stream = new StreamingOutput() {
          @Override
          public void write(OutputStream os) throws IOException, WebApplicationException {
            Writer writer = new BufferedWriter(new OutputStreamWriter(os));
            try {
              BufferedReader br = new BufferedReader(new InputStreamReader(HiveUIHdfsUtil.connectToHDFSApi().open(savedQuery.getQueryFile())));
              String line;
              line = br.readLine();
              while (line != null) {
                writer.write(line + "\n");
                line = br.readLine();
              }
              writer.flush();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }catch (Exception e1){
              e1.printStackTrace();
            } finally {
              writer.close();
            }
          }
        };
        return Response.ok(stream).
                type(MediaType.TEXT_PLAIN).
                build();
      } else {
        JSONObject object = new JSONObject();
        object.put("savedQuery", savedQuery);
        return Response.ok(object).build();
      }
    } catch (Exception ex) {
      throw ex;
    }
  }

  /**
   * Delete single item
   */
  @DELETE
  @Path("{queryId}")
  public Response delete(@PathParam("queryId") String queryId) {
    try {
      SavedQueryResourceManager.delete(queryId);
      return Response.status(204).build();
    } catch (WebApplicationException ex) {
      throw ex;
    }
  }

  /**
   * Get all SavedQueries
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getList() {
    try {
      LOG.debug("Getting all SavedQuery");
      List allSavedQueries = SavedQueryResourceManager.readAll();

      JSONObject object = new JSONObject();
      object.put("savedQueries", allSavedQueries);
      return Response.ok(object).build();
    } catch (WebApplicationException ex) {
      throw ex;
    }
  }

  /**
   * Update item
   */
  @PUT
  @Path("{queryId}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response update(SavedQueryRequest request,
                         @PathParam("queryId") String queryId) {
    if(SavedQueryResourceManager.update(request.savedQuery, queryId) != null){
      return Response.status(204).build();
    }else{
      return Response.status(500).build();
    }
  }

  /**
   * Create savedQuery
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(SavedQueryRequest request, @Context HttpServletResponse response,
                         @Context UriInfo ui) {
      SavedQueryResourceManager.create(request.savedQuery);
      SavedQuery item = null;
      item = SavedQueryResourceManager.read(request.savedQuery.getId());
      if(item != null){
        response.setHeader("Location",
                String.format("%s/%s", ui.getAbsolutePath().toString(), request.savedQuery.getId()));

        JSONObject object = new JSONObject();
        object.put("savedQuery", item);
        return Response.ok(object).status(201).build();
      }else{
        return Response.status(500).build();
      }


  }

  /**
   * Get default settings for query
   */
  @GET
  @Path("defaultSettings")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDefaultSettings() {
    try {
      //String defaultsFile = context.getProperties().get("scripts.settings.defaults-file");
      String defaultsFile = "/user/admin.defaultSettings";
      HiveUIHdfsApi hiveUIHdfsApi = HiveUIHdfsUtil.connectToHDFSApi();

      String defaults = "{\"settings\": {}}";
      if (hiveUIHdfsApi.exists(defaultsFile)) {
        defaults = HiveUIHdfsUtil.readFile(hiveUIHdfsApi, defaultsFile);
      }
      return Response.ok(JSONValue.parse(defaults)).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Set default settings for query (overwrites if present)
   */
  @POST
  @Path("defaultSettings")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setDefaultSettings(JSONObject settings) {
    try {
      //String defaultsFile = context.getProperties().get("scripts.settings.defaults-file");
      String defaultsFile = "/user/admin.defaultSettings";
      HiveUIHdfsApi hiveUIHdfsApi = HiveUIHdfsUtil.connectToHDFSApi();

      HiveUIHdfsUtil.putStringToFile(hiveUIHdfsApi, defaultsFile,
          settings.toString());
      String defaults = HiveUIHdfsUtil.readFile(hiveUIHdfsApi, defaultsFile);
      return Response.ok(JSONValue.parse(defaults)).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Wrapper object for json mapping
   */
  public static class SavedQueryRequest {
    public SavedQuery savedQuery;
  }
}
