package com.vikkarma.hbase.operations;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.vikkarma.hbase.connector.HbaseConnectionManager;
import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.PhoenixTestProperties;
import com.vikkarma.utils.PhoenixUtils;


public class HbaseOperations {

  private static final HBaseTestProperties hbaseTestProperties = HBaseTestProperties.getInstance();
  private static final boolean SCAN_ALL_RECORDS = hbaseTestProperties
      .getBoolean("SCAN_ALL_RECORDS");
  private static final int PS_QUERY_TIMEOUT = hbaseTestProperties
      .getInteger("PS_QUERY_TIMEOUT");
  private static final boolean ENABLE_SCAN_CACHE = hbaseTestProperties
      .getBoolean("ENABLE_SCAN_CACHE");
  private static final int READ_CACHE_BLOCK_MULTIPLIER = hbaseTestProperties
      .getInteger("READ_CACHE_BLOCK_MULTIPLIER");
  private static HbaseConnectionManager HBasePoolManager = null;
  
  //SQL Statements
  /*
  private static final PhoenixTestProperties phxTestProperties = PhoenixTestProperties.getInstance();
  private static final String SQL_CREATE_TABLE_IF_NOT_EXISTS = phxTestProperties.get("SQL_CREATE_TABLE_IF_NOT_EXISTS");
  private static final String SQL_DELETE_TABLE = phxTestProperties.get("SQL_DELETE_TABLE");
  private static final String SQL_UPSERT_HBASE_DATA = phxTestProperties.get("SQL_UPSERT_HBASE_DATA");
  private static final String SQL_QUERY_HBASE_DATA = phxTestProperties.get("SQL_QUERY_HBASE_DATA");
  */
  private static final String TABLE_NAME_TEMPLATE = "{TABLE_NAME_GOES_HERE}";
  private static final String SQL_CREATE_TABLE_IF_NOT_EXISTS =
      "CREATE TABLE IF NOT EXISTS " + TABLE_NAME_TEMPLATE + " (\n" + "  ROW_KEY_ID VARCHAR NOT NULL,\n"
          + "  ROW_VAL VARCHAR,\n" + "  CONSTRAINT PK PRIMARY KEY (ROW_KEY_ID)\n"
          + ") DEFERRED_LOG_FLUSH=true,SALT_BUCKETS=16";
  private static final String SQL_DELETE_TABLE = "DROP TABLE " + TABLE_NAME_TEMPLATE;
  private static final String SQL_UPSERT_HBASE_DATA =
      "UPSERT INTO " + TABLE_NAME_TEMPLATE +
      " (ROW_KEY_ID, ROW_VAL)" +
      " VALUES (?, ?)";
  private static final String SQL_QUERY_HBASE_DATA =
   "SELECT * FROM "  + TABLE_NAME_TEMPLATE + " WHERE ROW_KEY_ID LIKE ?";
  private static final String SQL_QUERY_HBASE_DATA1 =
      "SELECT * FROM "  + TABLE_NAME_TEMPLATE + " WHERE ROW_KEY_ID > '?' AND WHERE ROW_KEY_ID > '?'";

  
  // ****************************************
  // Initialize Hbase connection manager with
  // LightPool Htable or Normal Htable pool
  // ****************************************
  public static void initHbaseConnectionManager() {
    HBasePoolManager = new HbaseConnectionManager(); 
    MyLogger.mylogger.info("SQL_CREATE_TABLE_IF_NOT_EXISTS "+SQL_CREATE_TABLE_IF_NOT_EXISTS);
    MyLogger.mylogger.info("SQL_DELETE_TABLE "+SQL_DELETE_TABLE);
    MyLogger.mylogger.info("SQL_UPSERT_HBASE_DATA "+SQL_UPSERT_HBASE_DATA);
    MyLogger.mylogger.info("SQL_QUERY_HBASE_DATA "+SQL_QUERY_HBASE_DATA);
  }

  /**
   * Create the test table with replication enabled in all the test Hbase instances
   * @param tableName
   * @param columnFamilys
   * @param isReplicationEnabled
   * @throws Exception
   */
  public static void createTable(String tableName, String[] columnFamilys,
      boolean isReplicationEnabled) throws Exception {

    Connection conn = null;
    PreparedStatement ps = null;
    List<String> tableOptions = new ArrayList<String>();
    // -O SALT_BUCKETS=16
    StringBuilder createTableSql = new StringBuilder();
    try {
      createTableSql.append(SQL_CREATE_TABLE_IF_NOT_EXISTS);
      for (String to : tableOptions) {
        createTableSql.append(", ").append(to);
      }
      StringBuilder preparedSqlTableStmt = PhoenixUtils.replaceTableName(createTableSql);
      MyLogger.mylogger.info("Executing SQL: " + preparedSqlTableStmt.toString());

        // Create Table
        long beforetime = System.currentTimeMillis();
        conn = HBasePoolManager.getHBaseConnection();
        ps = conn.prepareStatement(preparedSqlTableStmt.toString());
        ps.execute();
        long dbOptime = System.currentTimeMillis() - beforetime;
        MyLogger.mylogger.info("HBase DB Operation HBase Action="
            + "createTable " + tableName + " StartTime=" + beforetime + " DBOperationTime="
            + dbOptime);
    } catch (SQLException e) {
      MyLogger.mylogger.severe("Could not create table using DDL: " + createTableSql + e.getMessage() + Arrays.toString(e.getStackTrace()));
    } finally {
      tryClose(ps);
      tryClose(conn);
    }
  }
  
  public static void addRecordBatchDirect(String tableName, List<HBaseRecord> batch) throws Exception {
    long beforetime = System.currentTimeMillis();
    Connection conn = HBasePoolManager.getHBaseConnection();
    MyLogger.mylogger.info("Put Batch Starting from RowKey="+batch.get(0).getKey());
    for (int i=0; i<batch.size(); i++) {
      addRecord(conn, batch.get(i).getKey(), batch.get(i).getVal());
    }
    conn.commit();
    tryClose(conn);
    long dbOptime = System.currentTimeMillis() - beforetime;
    MyLogger.mylogger.info("HBase DB Operation : Action=" + "addRecordBatchDirect startKey=" + batch.get(0).getKey() + " StartTime="
        + beforetime + " DBOperationTime=" + dbOptime + " Write batch size=" + batch.size()
        + " Batch write throughput=" + batch.size() / dbOptime + " KRows/sec");
  }
  
  public static void addRecord(Connection conn, String rowKey, String rowVal) {
    PreparedStatement ps = null;
    StringBuilder upsertSql = new StringBuilder();
    try {
      upsertSql.append(SQL_UPSERT_HBASE_DATA);
      StringBuilder preparedSqlTableStmt = PhoenixUtils.replaceTableName(upsertSql);      
      ps = conn.prepareStatement(preparedSqlTableStmt.toString());
      //Add max timeout of 10 seconds since some threads are hanging indefinitely
      //Looks like feature is not supported in Phoenix
      //ps.setQueryTimeout(PS_QUERY_TIMEOUT);
      ps.setString(1, rowKey);
      ps.setString(2, rowVal);
      ps.execute();
    } catch (SQLException e) {
      MyLogger.mylogger.severe("Could not upsert row using SQL: " + upsertSql.toString() + e.getMessage() + Arrays.toString(e.getStackTrace()));
    } finally {
      tryClose(ps);
    }
  }
  
  public static void getRecordBatchDirect(String tableName, List<HBaseRecord> batch) throws Exception {
    long beforetime = System.currentTimeMillis();
    Connection conn = HBasePoolManager.getHBaseConnection();
    String startRowKey = batch.get(0).getKey();
    String[] parts = startRowKey.split(":");
    String rowKeyLike = parts[0]+":"+parts[1];
     //getRecord(conn, batch.get(0).getKey(), batch.get(batch.size()-1).getKey());
    MyLogger.mylogger.info("Get from RowKey="+batch.get(0).getKey() + ":to:" + "RowKey=" + batch.get(batch.size()-1).getKey()+ " where rowkey is like "+ rowKeyLike);
    getRecord(conn, rowKeyLike);
    conn.commit();    
    long dbOptime = System.currentTimeMillis() - beforetime;
    MyLogger.mylogger.info("HBase DB Operation : Action=" + "getRecordBatchDirect startKey=" + " StartTime="
        + beforetime + " DBOperationTime=" + dbOptime + " Scan batch size=" + batch.size()
        + " Batch read throughput=" + batch.size() / dbOptime + " KRows/sec");
    tryClose(conn);
  }
  
  public static void getRecord(Connection conn, String rowKeyLike) {
    PreparedStatement ps = null;
    StringBuilder scanSql = new StringBuilder();
    try {
      scanSql.append(SQL_QUERY_HBASE_DATA);
      StringBuilder preparedSqlTableStmt = PhoenixUtils.replaceTableName(scanSql);
      MyLogger.mylogger.finer("Preparing SQL: " + preparedSqlTableStmt.toString());
      ps = conn.prepareStatement(scanSql.toString());
      //Looks like feature is not supported in Phoenix
      //ps.setQueryTimeout(PS_QUERY_TIMEOUT);
      ps.setString(1, rowKeyLike + "%");
      //ps.execute();
      ResultSet rs = ps.executeQuery();
      //MyLogger.mylogger.info("executeQuery rowkeyLike="+rowKeyLike+" GotResult="+rs.next()+ " Result="+rs.toString());
      int resultCtr = 0;
      while (rs.next()) {
        resultCtr++;
      }
      MyLogger.mylogger.info("executeQuery rowkeyLike="+rowKeyLike+" GotResult="+rs.next()+ " Fetched records="+resultCtr);

    } catch (SQLException e) {
      MyLogger.mylogger.severe("Could not scan row using SQL: " + scanSql.toString() + e.getMessage() + Arrays.toString(e.getStackTrace()));
    } finally {
      tryClose(ps);
    }
  }
  
  public static void getRecord(Connection conn, String startRowKey, String endRowKey) {
    PreparedStatement ps = null;
    StringBuilder scanSql = new StringBuilder();
    try {
      scanSql.append(SQL_QUERY_HBASE_DATA);
      StringBuilder preparedSqlTableStmt = PhoenixUtils.replaceTableName(scanSql);
      MyLogger.mylogger.finer("Preparing SQL: " + preparedSqlTableStmt.toString());
      ps = conn.prepareStatement(scanSql.toString());
      ps.setString(1, startRowKey);
      ps.setString(2, endRowKey);
      ps.execute();
    } catch (SQLException e) {
      MyLogger.mylogger.severe("Could not scan row using SQL: " + scanSql.toString() + e.getMessage() + Arrays.toString(e.getStackTrace()));
    } finally {
      tryClose(ps);
    }
  }


  public static void dropTable(String tableName) throws Exception {

    Connection conn = null;
    PreparedStatement ps = null;
    StringBuilder dropTableSql = new StringBuilder();
    try {
      dropTableSql.append(SQL_DELETE_TABLE);
      StringBuilder preparedSqlTableStmt = PhoenixUtils.replaceTableName(dropTableSql);
      MyLogger.mylogger.info("Executing SQL: " + preparedSqlTableStmt.toString());

        // Create Table
        long beforetime = System.currentTimeMillis();
        conn = HBasePoolManager.getHBaseConnection();
        ps = conn.prepareStatement(preparedSqlTableStmt.toString());
        ps.execute();
        long dbOptime = System.currentTimeMillis() - beforetime;
        MyLogger.mylogger.info("HBase DB Operation HBase Action="
            + "dropTable " + tableName + " StartTime=" + beforetime + " DBOperationTime="
            + dbOptime);
    } catch (SQLException e) {
      MyLogger.mylogger.severe("Could not drop table using DDL: " + dropTableSql + e.getMessage() + Arrays.toString(e.getStackTrace()));
    } finally {
      tryClose(ps);
      tryClose(conn);
    }
  }
  /**
   * Filter rows based on rowkey start / stop scan
   * 
   * @param tableName
   * @param startscan
   * @param endscan
   * @throws IOException
   */
  public static void getRowKeyStartStopScanRecords(String tableName,
      String startScan, String endScan) throws IOException {
    /*
    HTableInterface table = hbaseConnectionManager.getHTableInterface(
        hbaseInstanceIndex, tableName);

    // Set Scanner Properties
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    if(ENABLE_SCAN_CACHE) {
      scan.setCaching(HBaseLoadParams.getWriteBatchSize()/READ_CACHE_BLOCK_MULTIPLIER);
    }
    scan.setStartRow(startScanBytes);   
    scan.setStopRow(endScanBytes);

    long beforetime = System.currentTimeMillis();
    ResultScanner scanner = table.getScanner(scan);
    long dbOptime = System.currentTimeMillis() - beforetime;
    MyLogger.mylogger.fine("getRowKeyStartStopScanRecords:: Starting Scan for rowkeys in the range from " +  new String(startScanBytes) + " to " + new String(endScanBytes));
    if (SCAN_ALL_RECORDS) {
      int scancounter=0;
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext())
      {
          Result next = iterator.next();
          next.getRow();
          next.getValue(Bytes.toBytes(HbaseConstanats.historyColumnFamily[0]), Bytes.toBytes(HbaseConstanats.historyColumnQualifier));
          scancounter++;
      }
      MyLogger.mylogger.fine("getRowKeyStartStopScanRecords:: Scanned " + scancounter + " records in the range  from " +  new String(startScanBytes) + " to " + new String(endScanBytes));
    }
    MyLogger.mylogger.fine("HBase DB Operation : Action="
        + "getRowKeyStartStopScanRecords StartTime=" + beforetime
        + " DBOperationTime=" + dbOptime);
      dbOptime = System.currentTimeMillis() - beforetime;
      MyLogger.mylogger.finest("HBase DB Operation : Action="
          + "getRowKeyStartStopScanRecords StartTime=" + beforetime
          + " resultIterationTime=" + dbOptime);
  */
  }

  private static void tryClose(PreparedStatement ps) {
    if (ps != null) {
      try {
        ps.close();
      } catch (SQLException e) {/* ignored */
      }
    }
  }

  private static void tryClose(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {/* ignored */
      }
    }
  }

}
