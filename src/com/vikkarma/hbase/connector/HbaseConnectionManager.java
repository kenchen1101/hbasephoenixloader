package com.vikkarma.hbase.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;

public class HbaseConnectionManager {
  private static final String JDBC_DRIVER_CLASS = "org.apache.phoenix.jdbc.PhoenixDriver";
  private static final String JDBC_URL_PREFIX = "jdbc:phoenix:";
  private static final String hbaseMasterHost = HbaseConnectionParams.getHbaseMasterHost();
  
  public HbaseConnectionManager() {
    MyLogger.mylogger.info("Initializing HBase Connection Pool for "+hbaseMasterHost);
  }
  /**
	 * initialize HBase connection pool 
	 * one connection pool per hbase test instance
	 */
	public Connection getHBaseConnection() {
    //Initialize configuration
   // Configuration conf = getHbaseConfig(hbaseMasterHost,
   //     HbaseConnectionParams.getHbasePort());
    //Initialize Hconnection
	  Configuration conf = HBaseConfiguration.create();
          return (createConnection(hbaseMasterHost,conf));
	}
	
  private Connection createConnection(String zkQuorum, Configuration conf) {
    try {
      Class.forName(JDBC_DRIVER_CLASS);
    } catch (ClassNotFoundException e) {
      MyLogger.mylogger.severe(JDBC_DRIVER_CLASS + " not found"+ e.getMessage() + Arrays.toString(e.getStackTrace()));
      System.exit(-1);
      //throw new RuntimeException(JDBC_DRIVER_CLASS + " not found", e);
    }
    try {
      return DriverManager.getConnection(JDBC_URL_PREFIX + zkQuorum);
    } catch (SQLException e) {
      MyLogger.mylogger.severe("Could not get connection to " + zkQuorum + e.getMessage() + Arrays.toString(e.getStackTrace()));
      //throw new RuntimeException("Could not get connection to " + zkQuorum, e);
      System.exit(-1);
    }
    return null;
  }
	
	 /**
   * Initialize Hbase connection configuration
   * @param hbaseHost
   * @param hbasePort
   * @return
   */
  protected Configuration getHbaseConfig(String hbaseHost,
      String hbasePort) {

      HBaseTestProperties testProperties = HBaseTestProperties.getInstance();
    
    // Initialize configuration for normal Htable pools
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", hbaseHost);
    conf.set("hbase.zookeeper.property.clientPort", hbasePort);
    
    initProperty(conf, testProperties, "hbase.client.retries.number", String.valueOf(HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER));
    // NOTE: This doesn't appear anymore in the latest hbase version (0.9 had a constant named HBASE_CLIENT_RPC_MAXATTEMPTS which defaulted to 1 anyway)
    initProperty(conf, testProperties, "hbase.client.rpc.maxattempts", "1");
    initProperty(conf, testProperties, "zookeeper.recovery.retry", "0");
    initProperty(conf, testProperties, "hbase.client.pause", String.valueOf(HConstants.DEFAULT_HBASE_CLIENT_PAUSE));
    initProperty(conf, testProperties, "zookeeper.session.timeout", "30000");
    initProperty(conf, testProperties, "hbase.rpc.timeout", "5000");
    initProperty(conf, testProperties, "zookeeper.recovery.retry.intervalmill", "100");
    
    MyLogger.mylogger.info("Hbase client retry configs hbase.client.retries.number:" + conf.get("hbase.client.retries.number") + "hbase.client.rpc.maxattempts:" + conf.get("hbase.client.rpc.maxattempts") + "zookeeper.recovery.retry:" + conf.get("zookeeper.recovery.retry"));
    MyLogger.mylogger.info("Hbase client timeout configs hbase.client.pause:" + conf.get("hbase.client.pause") + "zookeeper.session.timeout:" + conf.get("zookeeper.session.timeout") + "hbase.rpc.timeout:" + conf.get("hbase.rpc.timeout") + "zookeeper.recovery.retry.intervalmill:" + conf.get("zookeeper.recovery.retry.intervalmill"));

    return conf;
  }
  
  private void initProperty(Configuration conf, HBaseTestProperties testProperties, String prop, String defVal) {
      String val = testProperties.get(prop, defVal);
      // Allow setting a blank value in the test properties to force a default value for it.
      // This is equivalent to the property being missing without having to physically remove it from the properties file.
      // This makes it easy to override the property from command-line with the default value, without actually knowing what the default is.
      if (val != defVal) {
          if (val != null && val.trim().equals("")) {
              // Don't set this property.
              return;
          }
          if (val != null && val.trim().equals("DEFAULT")) {
              val = defVal;
          }
      }
        conf.set(prop, val);
    }
}
