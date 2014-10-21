package com.vikkarma.phoenix.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


public class test {
  //private static final String JDBC_DRIVER_CLASS = "com.vikkarma.phoenix.jdbc.PhoenixDriver";
  private static final String JDBC_DRIVER_CLASS = "org.apache.phoenix.jdbc.PhoenixDriver";
  private static final String JDBC_URL_PREFIX = "jdbc:phoenix:";

    public static void main(String[] args) throws SQLException {
        Statement stmt = null;
        ResultSet rset = null;
        
        Configuration conf = HBaseConfiguration.create();
        Connection con = getConnection(conf);       
        
        stmt = con.createStatement();
        stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
        stmt.executeUpdate("upsert into test values (1,'Hello')");
        stmt.executeUpdate("upsert into test values (2,'World!')");
        con.commit();

        PreparedStatement statement = con.prepareStatement("select * from test");
        rset = statement.executeQuery();
        while (rset.next()) {
            System.out.println(rset.getString("mycolumn"));
            //System.out.println("...);
        }
        statement.close();
        con.close();
    }
    
    private static Connection getConnection(Configuration conf) {
      //String quorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
      String quorum = "localhost";
      try {
        Class.forName(JDBC_DRIVER_CLASS);
      } catch (ClassNotFoundException e) {
        //log.error(JDBC_DRIVER_CLASS + " not found", e);
        throw new RuntimeException(JDBC_DRIVER_CLASS + " not found", e);
      }
      try {
        return DriverManager.getConnection(JDBC_URL_PREFIX + quorum);
      } catch (SQLException e) {
        //log.error("Could not get connection to " + quorum, e);
        throw new RuntimeException("Could not get connection to " + quorum, e);
      }
    }
}

