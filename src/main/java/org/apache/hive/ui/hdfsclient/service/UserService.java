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

package org.apache.hive.ui.hdfsclient.service;

import org.apache.hive.ui.hdfsclient.help.HiveUIHdfsApi;
import org.apache.hive.ui.utils.NotFoundFormattedException;
import org.apache.hive.ui.utils.ServiceFormattedException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileNotFoundException;

/**
 * User related info service
 */
public class UserService extends HdfsService {



  /**
   * Returns home directory
   * @return home directory
   */
  @GET
  @Path("/home")
  @Produces(MediaType.APPLICATION_JSON)
  public Response homeDir() {
    try {
      HiveUIHdfsApi api = getApi();
      return Response
        .ok(getApi().fileStatusToJSON(api.getFileStatus(api.getHomeDir()
          .toString()))).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Is trash enabled
   * @return is trash enabled
   */
  @GET
  @Path("/trash/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public Response trashEnabled() {
    try {
      HiveUIHdfsApi api = getApi();
      return Response.ok(new FileOperationResult(api.trashEnabled())).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  /**
   * Trash dir
   * @return trash dir
   */
  @GET
  @Path("/trashDir")
  @Produces(MediaType.APPLICATION_JSON)
  public Response trashdir() {
    try {
      HiveUIHdfsApi api = getApi();
      return Response.ok(
        getApi().fileStatusToJSON(api.getFileStatus(api.getTrashDir()
          .toString()))).build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (FileNotFoundException ex) {
      throw new NotFoundFormattedException(ex.getMessage(), ex);
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }
}
