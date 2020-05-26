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

package org.apache.hive.ui.resources.udfs;
import org.apache.hive.ui.persistence.service.HiveUIUDFService;
import org.apache.hive.ui.persistence.po.HiveUIUDFEntity;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * Servlet for UDFs
 * API:
 * GET /:id
 *      read udf
 * POST /
 *      create new udf
 * GET /
 *      get all udf of current user
 */

@Path("/udfs")
public class UDFService {
  protected final Logger LOG =
          LoggerFactory.getLogger(getClass());

  /**
   * Get single item
   */
  @GET
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOne(@PathParam("id") String id) {

    HiveUIUDFEntity savedHiveUIUDFEntity  = HiveUIUDFService.findUDFById(Integer.parseInt(id));
    UDF item = new UDF();
    if(savedHiveUIUDFEntity != null){
      item.setId(savedHiveUIUDFEntity.getId().toString());
      item.setClassname(savedHiveUIUDFEntity.getClassname());
      item.setFileResource(savedHiveUIUDFEntity.getFileResource());
      item.setName(savedHiveUIUDFEntity.getName());
      item.setOwner(savedHiveUIUDFEntity.getOwner());
      JSONObject object = new JSONObject();
      object.put("udf", item);
      return Response.ok(object).build();
    }else{
      JSONObject object = new JSONObject();
      return Response.ok(object).build();
    }

  }

  /**
   * Delete single item
   */
  @DELETE
  @Path("{id}")
  public Response delete(@PathParam("id") String id) {
    HiveUIUDFService.deleteHiveUIUDF(HiveUIUDFService.findUDFById(Integer.parseInt(id)));
    return Response.status(204).build();

  }

  /**
   * Get all UDFs
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getList() {

      List<HiveUIUDFEntity> res = HiveUIUDFService.findUDFsByUserId(1);
      List<UDF> items = new ArrayList<UDF>();
      if(res != null && res.size()>0){
        for(HiveUIUDFEntity hiveUIUDFEntity :res){
          UDF item = new UDF();
          item.setId(hiveUIUDFEntity.getId().toString());
          item.setClassname(hiveUIUDFEntity.getClassname());
          item.setFileResource(hiveUIUDFEntity.getFileResource());
          item.setName(hiveUIUDFEntity.getName());
          item.setOwner(hiveUIUDFEntity.getOwner());
          item.setId(hiveUIUDFEntity.getId().toString());
          items.add(item);
        }
      }

      JSONObject object = new JSONObject();
      object.put("udfs", items);
      return Response.ok(object).build();

  }

  /**
   * Update item
   */
  @PUT
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response update(UDFRequest request,
                         @PathParam("id") String id) {

    HiveUIUDFEntity updatedHiveUIUDFEntity  = HiveUIUDFService.findUDFById(Integer.parseInt(id));
    updatedHiveUIUDFEntity.setUserID(1);
    updatedHiveUIUDFEntity.setClassname((request.udf.getClassname()));
    updatedHiveUIUDFEntity.setFileResource(request.udf.getFileResource());
    updatedHiveUIUDFEntity.setName(request.udf.getName());
    updatedHiveUIUDFEntity.setOwner(request.udf.getOwner());
    if(HiveUIUDFService.updateHiveUIUDF(updatedHiveUIUDFEntity)){
      return Response.status(204).build();
    };
    return Response.status(500).build();


  }

  /**
   * Create udf
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(UDFRequest request, @Context HttpServletResponse response,
                         @Context UriInfo ui) {

      HiveUIUDFEntity hiveUIUDFEntity = new HiveUIUDFEntity();
      hiveUIUDFEntity.setUserID(1);
      hiveUIUDFEntity.setClassname(request.udf.getClassname());
      hiveUIUDFEntity.setFileResource(request.udf.getFileResource());
      hiveUIUDFEntity.setName(request.udf.getName());
      hiveUIUDFEntity.setOwner("admin");

      Integer id = HiveUIUDFService.saveHiveUIUDF(hiveUIUDFEntity);
      if(id != null){
        System.out.println("savedHiveUIUDFEntity id:"+ id);
        HiveUIUDFEntity savedHiveUIUDFEntity  = HiveUIUDFService.findUDFById(id);
        UDF item = new UDF();
        item.setId(savedHiveUIUDFEntity.getId().toString());
        item.setClassname(savedHiveUIUDFEntity.getClassname());
        item.setFileResource(savedHiveUIUDFEntity.getFileResource());
        item.setName(savedHiveUIUDFEntity.getName());
        item.setOwner(savedHiveUIUDFEntity.getOwner());

        response.setHeader("Location",
                String.format("%s/%s", ui.getAbsolutePath().toString(), item.getId()));

        JSONObject object = new JSONObject();
        object.put("udf", item);
        return Response.ok(object).status(201).build();
      }else{
        JSONObject object = new JSONObject();
        return Response.ok(object).status(201).build();
      }


  }

  /**
   * Wrapper object for json mapping
   */
  public static class UDFRequest {
    public UDF udf;
  }
}
