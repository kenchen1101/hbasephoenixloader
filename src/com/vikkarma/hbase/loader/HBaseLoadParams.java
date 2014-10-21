package com.vikkarma.hbase.loader;

import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;

public class HBaseLoadParams {
	
	//Command line arguments
	private static int numHConn=0;
	private static int numWriters=0;	
	private static int numXchg=0;
	private static int numTopic=0;
	private static int numKeysPerET=0;
	private static int rowKeyDataLen=0;
	private static int writeBatchSize=0;
	
	//load Arguments	
	private static final HBaseTestProperties testProperties = HBaseTestProperties
			.getInstance();
	private static final int MAX_HTABLE_POOL_SIZE = testProperties
			.getInteger("MAX_HTABLE_POOL_SIZE"); //numHConn
	private static final int THREADPOOL_THREAD_COUNT = testProperties
			.getInteger("THREAD_COUNT"); //numWriters
	private static final int NUM_EXCHANGE = testProperties
			.getInteger("NUM_EXCHANGE");
	private static final int NUM_TOPICS = testProperties
			.getInteger("NUM_TOPICS");
	private static final int NUM_KEYS_PER_ET = testProperties
			.getInteger("NUM_KEYS_PER_ET");
	private static final int INSERT_BULK = testProperties
			.getInteger("INSERT_BULK"); //writeBatchSize
	private static final int MAX_DATA_LENGTH = testProperties
			.getInteger("MAX_DATA_LENGTH"); //rowKeyDataLen
	private static final int BATCH_MULTIPLE_BULK = testProperties.getInteger("BATCH_MULTIPLE_BULK");
	
  //Other test properties
	private static final boolean GENERATE_HISTORY_DATA = testProperties
      .getBoolean("GENERATE_HISTORY_DATA");
  private static final boolean RUNTIME_BATCH_GEN = testProperties.getBoolean("RUNTIME_BATCH_GEN");
  
	//NumKeys to be fetched from the data file cannot be 
	//greater than the number of keys in the file
	/*
	public static int getBulkSize() {
		int totalKeys = NUM_EXCHANGE * NUM_TOPICS * NUM_KEYS_PER_ET;
		if(BULK_SIZE < totalKeys) {
			return BULK_SIZE;
		}
		return totalKeys;
	}
	*/
	// Fetch a min of 1000 batches in one go else fetch all the keys
	public static int getBulkSize() {
		int totalKeys = NUM_EXCHANGE * NUM_TOPICS * NUM_KEYS_PER_ET;
		int bulkSize = INSERT_BULK*BATCH_MULTIPLE_BULK;
		if(bulkSize < totalKeys) {
			return bulkSize;
		}
		return totalKeys;
	}

	//HTable pool connection size
	public static int getNumHConn() {
		if(numHConn != 0) {
			return numHConn;
		}
		return MAX_HTABLE_POOL_SIZE;
	}
	public static void setNumHConn(int numHConn) {
		HBaseLoadParams.numHConn = numHConn;
	}
	
	//Num Writers
	public static int getNumWriters() {
		if(numWriters != 0) {
			return numWriters;
		}
		return THREADPOOL_THREAD_COUNT;
	}
	public static void setNumWriters(int numWriters) {
		HBaseLoadParams.numWriters = numWriters;
	}
	
	//Related to HBase data enables region splitting
	public static int getNumXchg() {
		if(numXchg != 0) {
			return numXchg;
		}
		return NUM_EXCHANGE;
	}
	public static void setNumXchg(int numXchg) {
		HBaseLoadParams.numXchg = numXchg;
	}
	
	//Related to HBase data enables region splitting
	public static int getNumTopic() {
		if(numTopic != 0) {
			return numTopic;
		}
		return NUM_TOPICS;
	}
	public static void setNumTopic(int numTopic) {
		HBaseLoadParams.numTopic = numTopic;
	}

	//Num keys per split data region
	public static int getNumKeysPerET() {
		if(numKeysPerET != 0) {
			return numKeysPerET;
		}
		return NUM_KEYS_PER_ET;
	}
	public static void setNumKeysPerET(int numKeysPerET) {
		HBaseLoadParams.numKeysPerET = numKeysPerET;
	}
	
	//data size for each rowkey
	public static int getRowKeyDataLen() {
		if (rowKeyDataLen != 0) {
			return rowKeyDataLen;
		}
		return MAX_DATA_LENGTH;
	}
	public static void setRowKeyDataLen(int rowKeyDataLen) {
		HBaseLoadParams.rowKeyDataLen = rowKeyDataLen;
	}
	
	//size of batch for one write thread
	public static int getWriteBatchSize() {
		if(writeBatchSize != 0) {
			return writeBatchSize;
		}
		return INSERT_BULK;
	}
	public static void setWriteBatchSize(int writeBatchSize) {
		HBaseLoadParams.writeBatchSize = writeBatchSize;
	}
	

	/**
	 * Parse the load arguments
	 * @param args
	 */
  public static void getLoadArguments(final String[] args) {
    if (args.length < 1) {
      System.out.println("No arguments specified, Using default values for test");
    }

    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage("Printing help");
          System.exit(-1);
        }
        System.out.println("Parsing user argument " + cmd);
        final String numHConnStr = "--num_hconn=";
        if (cmd.startsWith(numHConnStr)) {
          HBaseLoadParams.setNumHConn(Integer.parseInt(cmd.substring(numHConnStr.length())));
          continue;
        }

        final String numWritersStr = "--num_writers=";
        if (cmd.startsWith(numWritersStr)) {
          HBaseLoadParams.setNumWriters(Integer.parseInt(cmd.substring(numWritersStr.length())));
          continue;
        }

        final String numXchgStr = "--num_xchg=";
        if (cmd.startsWith(numXchgStr)) {
          HBaseLoadParams.setNumXchg(Integer.parseInt(cmd.substring(numXchgStr.length())));
          continue;
        }

        final String numTopicStr = "--num_topic=";
        if (cmd.startsWith(numTopicStr)) {
          HBaseLoadParams.setNumTopic(Integer.parseInt(cmd.substring(numTopicStr.length())));
          continue;
        }
        final String numKeysPerETStr = "--num_keys_per_ET=";
        if (cmd.startsWith(numKeysPerETStr)) {
          HBaseLoadParams
              .setNumKeysPerET(Integer.parseInt(cmd.substring(numKeysPerETStr.length())));
          continue;
        }
        final String rowKeyDataLenStr = "--rowkey_data_len=";
        if (cmd.startsWith(rowKeyDataLenStr)) {
          HBaseLoadParams
              .setRowKeyDataLen(Integer.parseInt(cmd.substring(rowKeyDataLenStr.length())));
          continue;
        }
        final String writeBatchSizeStr = "--write_batch_size=";
        if (cmd.startsWith(writeBatchSizeStr)) {
          HBaseLoadParams.setWriteBatchSize(Integer.parseInt(cmd.substring(writeBatchSizeStr
              .length())));
          continue;
        }

        // A catch all for overriding the test properties via command-line.
        if (cmd.startsWith("+D") && cmd.length() > 2 && cmd.indexOf('=') != -1) {
          int keyValueSepIndex = cmd.indexOf("=");
          String key = cmd.substring(2, keyValueSepIndex);
          String value = cmd.substring(keyValueSepIndex + 1);
          MyLogger.mylogger.info("Overriding test property " + key + " with value " + value);
          testProperties.setProperty(key, value);
          continue;
        }

        printUsage("Invalid argument specified" + cmd);
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    MyLogger.mylogger.info("Validating load arguments");
    validateLoadArguments();
    
    System.out.println("Running load client with the following load params " + " num_hconn="
        + HBaseLoadParams.getNumHConn() + " num_writers=" + HBaseLoadParams.getNumWriters()
        + " num_xchg=" + HBaseLoadParams.getNumXchg() + " num_topic="
        + HBaseLoadParams.getNumTopic() + " num_keys_per_ET=" + HBaseLoadParams.getNumKeysPerET()
        + " rowkey_data_len=" + HBaseLoadParams.getRowKeyDataLen() + " write_batch_size="
        + HBaseLoadParams.getWriteBatchSize());
    MyLogger.mylogger.info("Running load client with the following load params " + " num_hconn="
        + HBaseLoadParams.getNumHConn() + " num_writers=" + HBaseLoadParams.getNumWriters()
        + " num_xchg=" + HBaseLoadParams.getNumXchg() + " num_topic="
        + HBaseLoadParams.getNumTopic() + " num_keys_per_ET=" + HBaseLoadParams.getNumKeysPerET()
        + " rowkey_data_len=" + HBaseLoadParams.getRowKeyDataLen() + " write_batch_size="
        + HBaseLoadParams.getWriteBatchSize());

  }
  
  /**
   * Validate load arguments and exit test 
   * if input arguments do not match any of the 
   * validation criteria with a warning message
   */
  private static void validateLoadArguments() {
    if (!GENERATE_HISTORY_DATA && RUNTIME_BATCH_GEN) {
      System.out.println("For RUNTIME_BATCH_GEN=true GENERATE_HISTORY_DATA must be set to true");
      MyLogger.mylogger
          .info("For RUNTIME_BATCH_GEN=true GENERATE_HISTORY_DATA must be set to true");
      System.exit(-1);
    }
    if (HBaseLoadParams.getNumKeysPerET() < HBaseLoadParams.getWriteBatchSize()) {
      System.out.println("NumKeysPerET cannot be less than WriteBatchSize");
      MyLogger.mylogger.info("NumKeysPerET cannot be less than WriteBatchSize");
      System.exit(-1);

    }
    if (HBaseLoadParams.getNumKeysPerET() % HBaseLoadParams.getWriteBatchSize() != 0) {
      System.out.println("NumKeysPerET shd be a multiple of WriteBatchSize");
      MyLogger.mylogger.info("NumKeysPerET shd be a multiple of WriteBatchSize");
      System.exit(-1);

    }
    if (HBaseLoadParams.getBulkSize() > HBaseLoadParams.getNumXchg()
        * HBaseLoadParams.getNumTopic() * HBaseLoadParams.getNumKeysPerET()) {
      System.out.println("bulk size to fetch from file cannot be less than total keys");
      MyLogger.mylogger.info("bulk size to fetch from file cannot be less than total keys");
      System.exit(-1);

    }
    if ((HBaseLoadParams.getNumXchg() * HBaseLoadParams.getNumTopic() * HBaseLoadParams
        .getNumKeysPerET()) % HBaseLoadParams.getWriteBatchSize() != 0) {
      System.out.println("total keys shd be a multiple of WriteBatchSize");
      MyLogger.mylogger.info("total keys shd be a multiple of WriteBatchSize");
      System.exit(-1);
    }
    if ((HBaseLoadParams.getNumXchg() * HBaseLoadParams.getNumTopic())
        % SplitRowKeyGenerator.NUM_REGION_SPLITS != 0) {
      System.out
          .println("total ET's shd be a multiple of NUM_REGION_SPLITS for unform distribution");
      MyLogger.mylogger
          .info("total ET's shd be a multiple of NUM_REGION_SPLITS for unform distribution");
      System.exit(-1);
    }
    
  }
  
  /**
   * Load tool usage and arguments
   * @param message
   */
  private static void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java com.vikkarma.hbase.loader.HBasePerfTestClientMax \\");
    System.err.println("  [--num_hconn=MAX_HTABLE_POOL_SIZE] [--num_writers=THREAD_COUNT]");
    System.err
        .println("  [--num_xchg=NUM_EXCHANGE] [--num_topic=NUM_TOPICS] [--num_keys_per_ET=NUM_KEYS_PER_ET]");
    System.err.println("  [--rowkey_data_len=MAX_DATA_LENGTH] [--write_batch_size=INSERT_BULK]");

    System.err.println();
    System.err.println("Options:");
    System.err.println(" num_hconn        Number of HTable connections pool");
    System.err.println(" num_writers        Number of threads in the HBase writer threadpool ");
    System.err.println(" num_xchg         Rowkey component used for rowkey splitting");
    System.err.println(" num_topic        Rowkey component used for rowkey splitting");
    System.err.println(" num_keys_per_ET      Rowkey component used for rowkey splitting");
    System.err.println(" rowkey_data_len      length of data in rowkey value");
    System.err.println(" write_batch_size      number of rows to be written in one batch");

    System.err.println();
    System.err.println("Command to run the test:");
    System.err.println();
    System.err
        .println("$PROJECT_HOME/scripts/LaunchSecureHBaseMaxPerfTestClient.sh --num_hconn=16 --num_writers=16 --num_xchg=80 --num_topic=80 --num_keys_per_ET=6400 --rowkey_data_len=200 --write_batch_size=6400");
    System.err.println("For more configurable parameters refer $PROJECT_HOME/config/HBaseTest.properties");
  }
}
