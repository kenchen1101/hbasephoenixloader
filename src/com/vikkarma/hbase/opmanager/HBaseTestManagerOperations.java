package com.vikkarma.hbase.opmanager;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.hbase.operations.HbaseOperations;
import com.vikkarma.utils.HBaseConstanats;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;

public class HBaseTestManagerOperations {
  private static final HBaseTestProperties testProperties = HBaseTestProperties.getInstance();
  private static final String TABLE_NAME = testProperties.get("TABLE_NAME");
  private static final boolean GENERATE_HISTORY_DATA = testProperties
      .getBoolean("GENERATE_HISTORY_DATA");
  // In case of DR cluster with replication enabled
  // HBaseTest.properties must contain MULTI_HBASE_HOST=Primary#DR
  private static final boolean IS_REPLICATION_ENABLED = testProperties
      .getBoolean("IS_REPLICATION_ENABLED");
  private static ThreadPoolExecutor hbaseTestManagerPool;
  private static final int TEST_MANAGER_THREAD_COUNT = 4;
  
  public static void initHBaseTestManagerOperations() {
     MyLogger.mylogger.info("Starting Test Manager Pool ....");
    hbaseTestManagerPool =
        (ThreadPoolExecutor) Executors.newFixedThreadPool(TEST_MANAGER_THREAD_COUNT);
    MyLogger.mylogger.info("Initializing Hbase Connection Manager ....");
    HbaseOperations.initHbaseConnectionManager();
  }
  
  /**
   * Start the threadpool manager for insertion of records into Hbase table
   * @param tableType
   * @param startInsertTime
   */
  public static void fetchHbaseRecords() {
    // Read from data file and insert into hbase
    long startInsertTime = System.currentTimeMillis();
    MyLogger.mylogger.info("Starting FetchPoolManager for fetching key batches ....");
    CountDownLatch cdl = new CountDownLatch(1);

    // There is a put batch generator thread that will generate the put batches and put it on Queue
    // HBase writer will read it from the queue and put it in HBase. Good for High throughput
    // long running test
    ReadLoadManager rlm = new FetchPoolManagerBatchRunTime(cdl);

    //execulte the read load manager thread
    Thread readManagerThread = new Thread(rlm);
    hbaseTestManagerPool.execute(readManagerThread);

    // Wait for the writer manager to shutdown
    try {
      cdl.await();
    } catch (InterruptedException e) {
      MyLogger.mylogger.severe("fetchHbaseRecords " + e.getMessage() + Arrays.toString(e.getStackTrace()));
    }

    // log time required
    long endInsertTime = System.currentTimeMillis();
    MyLogger.mylogger
        .info("Completed FetchPoolManager for fetching History messages starting from "
            + startInsertTime + " to " + endInsertTime);

  }

  /**
   * Start the threadpool manager for insertion of records into Hbase table
   * @param tableType
   * @param startInsertTime
   */
  public static void insertHbaseRecords() {
    // Read from data file and insert into hbase
    long startInsertTime = System.currentTimeMillis();
    MyLogger.mylogger
        .info("Starting InsertPoolManager for Insertion of HBase data batches ....");
    CountDownLatch cdl = new CountDownLatch(1);
    // There is a put batch generator thread that will generate the put batches and put it on Queue
    // HBase writer will read it from the queue and put it in HBase. Good for High throughput
    // long running test;;
    WriteLoadManager wlm = new InsertPoolManagerBatchRunTime(cdl);
    Thread writerManagerThread = new Thread(wlm);
    hbaseTestManagerPool.execute(writerManagerThread);

    // Wait for the writer manager to shutdown
    try {
      cdl.await();
    } catch (InterruptedException e) {
      MyLogger.mylogger.severe("insertHbaseRecords " + e.getMessage() + Arrays.toString(e.getStackTrace()));
    }

    // log time required
    long endInsertTime = System.currentTimeMillis();
    MyLogger.mylogger
        .info("Completed InsertPoolManager for Insertion of History messages starting from "
            + startInsertTime + " to " + endInsertTime);

  }
  
  /**
   * Create Hbase table, add region splits if required
   * @param tableType
   */
  public static void testCreateTable() {
    MyLogger.mylogger.info("Creating hbase load test table " + TABLE_NAME + "....");
    long startInsertTime = System.currentTimeMillis();
    byte[][] splitKeys;
    if (GENERATE_HISTORY_DATA) {
      // get the in-memory populated split key array
      // generated at the time of history data generation
      splitKeys = SplitRowKeyGenerator.getSplitKeys();
    } else {
      // get the split keys from the key file
      splitKeys = SplitRowKeyGenerator.getRegionSplitsFromKeyFile();
    }
    try {
      HbaseOperations.createTable(TABLE_NAME, HBaseConstanats.historyColumnFamily,
        IS_REPLICATION_ENABLED);
    } catch (Exception e) {
      MyLogger.mylogger.severe("testCreateTable " + e.getMessage() + Arrays.toString(e.getStackTrace()));
      System.out.println("Table creation failed, exiting test ....");
      System.exit(-1);
    }
    long endInsertTime = System.currentTimeMillis() - startInsertTime;
    MyLogger.mylogger.info("Completed Creation hbase load test table " + TABLE_NAME  + " start time " + startInsertTime
        + " and completed in " + endInsertTime + " ms");
  }

  /**
   * Delete the given Hbase table
   * @param tableName
   */
  public static void deleteHbaseTable() {
    MyLogger.mylogger.info("Deleting " + TABLE_NAME + "....");
    try {
      HbaseOperations.dropTable(TABLE_NAME);
    } catch (Exception e) {
      MyLogger.mylogger.severe("deleteHbaseTable " + e.getMessage() + Arrays.toString(e.getStackTrace()));
    }
  }
  
  public static void shutdown() {
    MyLogger.mylogger.info("Shutting down Test Manager Pool ....");
    hbaseTestManagerPool.shutdown();
  }

}
