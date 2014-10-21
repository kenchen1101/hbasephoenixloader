package com.vikkarma.utils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PhoenixUtils {
  private static final HBaseTestProperties testProperties = HBaseTestProperties.getInstance();
  private static final String TABLE_NAME_TEMPLATE = "{TABLE_NAME_GOES_HERE}";
  private static final String TABLE_NAME = testProperties.get("TABLE_NAME");
  
  /**
   * Do an in-place replacement of the tablename, if DEF_TABLE_NAME is found and return the object for call chaining.
   * @param sql
   * @return parameter
   */
  public static StringBuilder replaceTableName(StringBuilder sql) {
    int idx = sql.indexOf(TABLE_NAME_TEMPLATE);
    if (idx != -1) {
      sql.replace(idx, idx+TABLE_NAME_TEMPLATE.length(), TABLE_NAME);
    }
    return sql;
  }
  
  public static void waitForTask(final List<Future<?>> list, final long timeout, final TimeUnit timeUnit) {
    for (final Future<?> future : list) {
      try {
        future.get(timeout, timeUnit);
      } catch (final TimeoutException e) {
        MyLogger.mylogger.severe("Cancelling thread since execution exceeds "+timeout + " seconds");
        future.cancel(true);
      } catch (final Exception e) {
        MyLogger.mylogger.severe("Future get failed: "+ e.getMessage() + Arrays.toString(e.getStackTrace()));
      }
    }
  }

}
