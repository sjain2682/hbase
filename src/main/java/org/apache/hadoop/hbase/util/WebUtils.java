package org.apache.hadoop.hbase.util;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HttpServer;

public class WebUtils {
  protected static final Log LOG = LogFactory.getLog(WebUtils.class);
  private static volatile String HTTP_URL_SCHEME = null;
  public static String getHttpUrlScheme() {
    if (HTTP_URL_SCHEME == null) {
      synchronized (WebUtils.class) {
        if (HTTP_URL_SCHEME == null) {
          try {
            Class<HttpServer> httpServerClass = org.apache.hadoop.http.HttpServer.class;
            Method getUrlScheme = httpServerClass.getMethod("getUrlScheme");
            String scheme = (String) getUrlScheme.invoke(null);
            HTTP_URL_SCHEME = scheme;
          } catch (NoSuchMethodException e) {
            LOG.debug("org.apache.hadoop.http.HttpServer.getUrlScheme() not available.");
          } catch (Exception e) {
            LOG.warn("Error determining HTTP Url scheme, using Http://", e);
          }
          finally {
            if (HTTP_URL_SCHEME == null) {
              HTTP_URL_SCHEME = "http://";
            }
          }
        }
      }
    }
    return HTTP_URL_SCHEME;
  }
}
