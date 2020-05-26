package org.apache.hive.ui.resources.jobs.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.core.Response;
public class HttpUtil {
    protected final static Logger LOG =
            LoggerFactory.getLogger(HttpUtil.class);

    public static InputStream getInputStream(String spec, String requestMethod)
            throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(spec).openConnection();
        connection.setRequestMethod(requestMethod);
        int responseCode = connection.getResponseCode();
        LOG.info("ATSUtil: request :" + spec  + " responseCode:" + responseCode);
        return responseCode >= Response.Status.BAD_REQUEST.getStatusCode() ?
                connection.getErrorStream() : connection.getInputStream();
    }







}
