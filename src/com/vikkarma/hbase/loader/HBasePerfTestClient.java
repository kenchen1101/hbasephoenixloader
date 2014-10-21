package com.vikkarma.hbase.loader;

import java.util.Arrays;

import com.vikkarma.hbase.data.HBaseDatagenerator;
import com.vikkarma.hbase.opmanager.HBaseTestManagerOperations;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.ShutDownHook;

/**
 * Loader class to load test configurations/arguments and trigger the hbase load test
 */
public class HBasePerfTestClient {
  private static final HBaseTestProperties testProperties = HBaseTestProperties.getInstance();
  private static final String ZK_QUORUM = testProperties.get("MULTI_HBASE_HOST");
  private static final String TABLE_NAME = testProperties.get("TABLE_NAME");
  private static final boolean DELETE_EXISTING_TABLE = testProperties
      .getBoolean("DELETE_EXISTING_TABLE");
  private static final boolean READ_DATA = testProperties.getBoolean("READ_DATA");
  private static final boolean READ_ONLY = testProperties.getBoolean("READ_ONLY");
  private static final boolean REPEAT_DATA = testProperties.getBoolean("REPEAT_DATA");
  private static final boolean GENERATE_HISTORY_DATA = testProperties
      .getBoolean("GENERATE_HISTORY_DATA");
  private static int SLEEP_AFTER_WRITE_BEFORE_READ_MS = testProperties
      .getInteger("SLEEP_AFTER_WRITE_BEFORE_READ_MS");

  /**
   * HBase Perf Test Main Class
   * @param args
   */
  public static void main(String[] args) {

    System.out.println("Starting HBase test, connecting to Zookeeper Quorum " + ZK_QUORUM + "....");
    MyLogger.mylogger.info("Starting HBase test, connecting to Zookeeper Quorum " + ZK_QUORUM
        + "....");

    // ****************************************
    // Create a shutdown hook
    // ****************************************
    MyLogger.mylogger.info("Adding ShutDown Hook thread ....");
    ShutDownHook sh = new ShutDownHook();
    Runtime.getRuntime().addShutdownHook(sh);
    ShutDownHook.setWaitTime(200);

    // ****************************************
    // Process command line arguments
    // ****************************************
    HBaseLoadParams.getLoadArguments(args);

    // ****************************************
    // Initialize Hbase Test manager
    // ****************************************
    MyLogger.mylogger.info("Initializing Hbase Test Manager ....");
    HBaseTestManagerOperations.initHBaseTestManagerOperations();

    // ****************************************
    // generate hbase data set and put it in a
    // test file in sgdata/hbase_test_data.csv
    // ****************************************
    if (GENERATE_HISTORY_DATA) {
      generateTestDataFile();
    }

    do {

      // ****************************************
      // Delete Hbase Test Tables if already exists
      // ****************************************
      if (DELETE_EXISTING_TABLE && !READ_ONLY) {
        MyLogger.mylogger.info("Deleting " + TABLE_NAME + "....");
        HBaseTestManagerOperations.deleteHbaseTable();
      }

      // ****************************************
      // Create Hbase Test Table
      // ****************************************
      HBaseTestManagerOperations.testCreateTable();

      // run hbase insert test if it is not a read only test
      if (!READ_ONLY) {
        HBaseTestManagerOperations.insertHbaseRecords();
      }

      // small sleep to let the system cool down after write tests
      if (READ_DATA || READ_ONLY) {
        try {
          Thread.sleep(SLEEP_AFTER_WRITE_BEFORE_READ_MS);
        } catch (InterruptedException e) {
          MyLogger.mylogger.severe("" + e.getMessage() + Arrays.toString(e.getStackTrace()));
        }
        // Fetch Test
        HBaseTestManagerOperations.fetchHbaseRecords();
      }

    } while (REPEAT_DATA);

    // Clean up and Shutdown test
    HBaseTestManagerOperations.shutdown();
    MyLogger.mylogger.info("Done !");
    System.exit(0);

  }

  /**
   * generate hbase data set and put it in a test file based on whether the fetch is based on rowkey
   * scan or column qualifier filtering
   */
  public static void generateTestDataFile() {
    MyLogger.mylogger.info("Generating data file for split table with batch inserts");
    HBaseDatagenerator hgen = new HBaseDatagenerator();
    hgen.generateSplitBatchHbaseDataFiles();
  }

}
