package com.vikkarma.phoenix.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class PhoenixConnectionUtil {
  public static Connection getPhxConnection() {
    Configuration conf = HBaseConfiguration.create();
    String zkQuorum = "blitzhbase01-mnds2-1-crd.eng.sfdc.net,blitzhbase01-mnds4-1-crd.eng.sfdc.net";
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    } catch (ClassNotFoundException e) {
      System.out.println("org.apache.phoenix.jdbc.PhoenixDriver not found");
      System.exit(-1);
    }
    try {
      return DriverManager.getConnection("jdbc:phoenix:" + zkQuorum);
    } catch (SQLException e) {
      System.out.println("Could not get connection to " + zkQuorum);
      System.exit(-1);
    }
    return null;
  }

}
