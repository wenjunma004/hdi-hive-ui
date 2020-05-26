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

package org.apache.hive.ui.resources.settings;

import org.apache.hive.ui.persistence.service.HiveUISettingService;
import org.apache.hive.ui.persistence.po.HiveUISettingEntity;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;

/**
 * Service to support the API call for basic CRUD operations of User Setting
 */

@Path("/settings")
public class SettingsService  {

  protected final Logger LOG =
      LoggerFactory.getLogger(getClass());


  /**
   * Gets all the settings for the current user
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAll() {
    List<HiveUISettingEntity> res = HiveUISettingService.findSettingsByUserId(1);
    List<Setting> items = new ArrayList<>();
    if(res != null && res.size()>0){
      for(HiveUISettingEntity entity: res){
        Setting setting = new Setting();
        setting.setKey(entity.getKey());
        setting.setValue(entity.getValue());
        setting.setId(entity.getId().toString());
        setting.setOwner(entity.getOwner());
        items.add(setting);
      }
    }
    JSONObject response = new JSONObject();
    response.put("settings", items);
    return Response.ok(response).build();
  }

  /**
   * Adds a setting for the current user
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response addSetting(SettingRequest settingRequest, @Context HttpServletResponse response, @Context UriInfo uriInfo) {
    HiveUISettingEntity newHiveUISettingEntity = new HiveUISettingEntity();
    newHiveUISettingEntity.setOwner("admin");
    newHiveUISettingEntity.setUserID(1);
    newHiveUISettingEntity.setKey(settingRequest.setting.getKey());
    newHiveUISettingEntity.setValue(settingRequest.setting.getValue());
    Integer id = HiveUISettingService.saveHiveUISetting(newHiveUISettingEntity);
    if(id != null){
      HiveUISettingEntity savedHiveUISettingEntity = HiveUISettingService.findSettingById(id);
      Setting setting = new Setting();
      if(savedHiveUISettingEntity != null){
        setting.setKey(savedHiveUISettingEntity.getKey());
        setting.setValue(savedHiveUISettingEntity.getValue());
        setting.setId(savedHiveUISettingEntity.getId().toString());
        setting.setOwner(savedHiveUISettingEntity.getOwner());
        response.setHeader("Location",
                String.format("%s/%s", uriInfo.getAbsolutePath().toString(), setting.getId()));

        JSONObject op = new JSONObject();
        op.put("setting", setting);
        return Response.ok(op).build();
      }

    }
    JSONObject op = new JSONObject();
    return Response.ok(op).build();

  }

  /**
   * Updates a setting for the current user
   */
  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateSetting(@PathParam("id") String id, SettingRequest settingRequest, @Context HttpServletResponse response, @Context UriInfo uriInfo) {

    HiveUISettingEntity updated = HiveUISettingService.findSettingById(Integer.parseInt(id));
    if (updated != null) {
      updated.setKey(settingRequest.setting.getKey());
      updated.setValue(settingRequest.setting.getValue());
      updated.setOwner(settingRequest.setting.getOwner());
      boolean res = HiveUISettingService.updateHiveUISetting(updated);
      if (res) {
        response.setHeader("Location",
                String.format("%s/%s", uriInfo.getAbsolutePath().toString(), updated.getId()));

        JSONObject op = new JSONObject();
        op.put("setting", settingRequest.setting);
        return Response.ok(op).build();
      }
    }
    return Response.status(500).build();
  }

    /**
     * Deletes a setting for the current user
     */
    @DELETE
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete (@PathParam("id") String id){

      HiveUISettingEntity deleted = HiveUISettingService.findSettingById(Integer.parseInt(id));
      if (deleted != null) {
        boolean res = HiveUISettingService.deleteHiveUISetting(deleted);
        if (res) {
          return Response.noContent().build();
        }
      }
      return Response.status(500).build();
    }

    /**
     * Wrapper class for settings request
     */
    public static class SettingRequest {
      private Setting setting;

      public Setting getSetting() {
        return setting;
      }

      public void setSetting(Setting setting) {
        this.setting = setting;
      }
    }
  }

