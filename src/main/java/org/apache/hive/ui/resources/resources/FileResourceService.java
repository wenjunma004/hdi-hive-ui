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

package org.apache.hive.ui.resources.resources;



import org.apache.hive.ui.persistence.service.HiveUIFileSourceItemService;
import org.apache.hive.ui.persistence.po.HiveUIFileSourceItemEntity;


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
 * Servlet for Resources
 * API:
 * GET /:id
 *      read resource
 * POST /
 *      create new resource
 * GET /
 *      get all resource of current user
 */
@Path("/fileResources")
public class FileResourceService  {
  protected final Logger LOG =
          LoggerFactory.getLogger(getClass());
  /**
   * Get single item
   */
  @GET
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOne(@PathParam("id") String id) {
    HiveUIFileSourceItemEntity savedHiveUIFileSourceItemEntity = HiveUIFileSourceItemService.findFileSourceItemById(Integer.parseInt(id));
    if(savedHiveUIFileSourceItemEntity != null){
      FileResourceItem item = new FileResourceItem();
      item.setOwner(savedHiveUIFileSourceItemEntity.getOwner());
      item.setName(savedHiveUIFileSourceItemEntity.getName());
      item.setPath(savedHiveUIFileSourceItemEntity.getPath());
      item.setId(savedHiveUIFileSourceItemEntity.getId().toString());
      JSONObject object = new JSONObject();
      object.put("fileResource", item);
      return Response.ok(object).build();
    }
    JSONObject object = new JSONObject();
    return Response.ok(object).status(201).build();


  }

  /**
   * Delete single item
   */
  @DELETE
  @Path("{id}")
  public Response delete(@PathParam("id") String id) {
    HiveUIFileSourceItemEntity del = HiveUIFileSourceItemService.findFileSourceItemById(Integer.parseInt(id));
    if(del != null){
      boolean res = HiveUIFileSourceItemService.deleteHiveUIFileSourceItem(del);
      if(res){
        return Response.status(204).build();
      }
    }
    return Response.status(500).build();

  }

  /**
   * Get all resources
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getList() {
    List<HiveUIFileSourceItemEntity> res = HiveUIFileSourceItemService.findFileSourceItemsByUserId(1);
    List<FileResourceItem> items = new ArrayList<>();
    if(res != null){
      for(HiveUIFileSourceItemEntity itemEntity:res){
        FileResourceItem item = new FileResourceItem();
        item.setOwner(itemEntity.getOwner());
        item.setName(itemEntity.getName());
        item.setPath(itemEntity.getPath());
        item.setId(itemEntity.getId().toString());
        items.add(item);
      }
    }

      JSONObject object = new JSONObject();
      object.put("fileResources", items);
      return Response.ok(object).build();

  }

  /**
   * Update item
   */
  @PUT
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response update(ResourceRequest request,
                         @PathParam("id") String id) {

    HiveUIFileSourceItemEntity update = HiveUIFileSourceItemService.findFileSourceItemById(Integer.parseInt(id));
    if(update != null){
      boolean res = HiveUIFileSourceItemService.updateHiveUIFileSourceItem(update);
      if(res){
        return Response.status(204).build();
      }
    }
    return Response.status(500).build();


  }

  /**
   * Create resource
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(ResourceRequest request, @Context HttpServletResponse response,
                         @Context UriInfo ui) {

      HiveUIFileSourceItemEntity newHiveUIFileSourceItemEntity = new HiveUIFileSourceItemEntity();
      newHiveUIFileSourceItemEntity.setOwner("admin");
      newHiveUIFileSourceItemEntity.setUserID(1);
      newHiveUIFileSourceItemEntity.setName(request.fileResource.getName());
      newHiveUIFileSourceItemEntity.setPath(request.fileResource.getPath());
      Integer id = HiveUIFileSourceItemService.saveHiveUIFileSourceItem(newHiveUIFileSourceItemEntity);
      if(id != null){
        HiveUIFileSourceItemEntity savedHiveUIFileSourceItemEntity = HiveUIFileSourceItemService.findFileSourceItemById(id);
        if(savedHiveUIFileSourceItemEntity != null){
          FileResourceItem item = new FileResourceItem();
          item.setOwner(savedHiveUIFileSourceItemEntity.getOwner());
          item.setName(savedHiveUIFileSourceItemEntity.getName());
          item.setPath(savedHiveUIFileSourceItemEntity.getPath());
          item.setId(savedHiveUIFileSourceItemEntity.getId().toString());
          response.setHeader("Location",
                  String.format("%s/%s", ui.getAbsolutePath().toString(), item.getId()));
          JSONObject object = new JSONObject();
          object.put("fileResource", item);
          return Response.ok(object).status(201).build();
        }
      }
    JSONObject object = new JSONObject();
    return Response.ok(object).status(201).build();

  }

  /**
   * Wrapper object for json mapping
   */
  public static class ResourceRequest {
    public FileResourceItem fileResource;
  }
}
