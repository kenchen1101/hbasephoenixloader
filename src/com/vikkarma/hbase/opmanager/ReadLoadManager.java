package com.vikkarma.hbase.opmanager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.TransactionSuccessCounter;

public abstract class ReadLoadManager implements Runnable {
  private static final int ROW_VAL_SIZE = HBaseLoadParams.getRowKeyDataLen();
  protected static int numPutBatches = HBaseLoadParams.getNumXchg() * HBaseLoadParams.getNumTopic()
      * HBaseLoadParams.getNumKeysPerET() / HBaseLoadParams.getWriteBatchSize();
  protected static TransactionSuccessCounter txnCtr = new TransactionSuccessCounter(numPutBatches);

  protected static ThreadPoolExecutor threadPool;
  protected CountDownLatch mgrlatch;

  public ReadLoadManager(CountDownLatch cdl) {
    mgrlatch = cdl;
    threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(HBaseLoadParams.getNumWriters());
  }

  public void cleanUp(long beforetime) {
    long totalTime = System.currentTimeMillis() - beforetime;
    long readTotalTput = (long) SplitRowKeyGenerator.TOTAL_KEYS / (long) totalTime;
    long readTotalTputMBs = (long)((long)(ROW_VAL_SIZE + 53))*readTotalTput/1024;
    MyLogger.mylogger.info("Expected:: Fetched from HBase " + SplitRowKeyGenerator.TOTAL_KEYS
        + " in " + numPutBatches + " batches. Total time required " + totalTime + " Throughput is "
        + readTotalTput + " KRows/sec " + readTotalTputMBs + " MB/sec");

    readTotalTput =
        (long) HBaseLoadParams.getWriteBatchSize() * txnCtr.getCount() / (long) totalTime;
    readTotalTputMBs = (long)((long)(ROW_VAL_SIZE + 53))*readTotalTput/1024;
    MyLogger.mylogger.info("Actual:: Fetched from HBase " + HBaseLoadParams.getWriteBatchSize()
        * txnCtr.getCount() + " in " + txnCtr.getCount() + " batches. Total time required "
        + totalTime + " Throughput is " + readTotalTput + " KRows/sec " + readTotalTputMBs + " MB/sec");

    MyLogger.mylogger.info("Shutting down ReadLoadManager threadpool ....");
    try {
      threadPool.shutdown();
    } finally {
      mgrlatch.countDown();
    }
  }

  public void cleanUp() {
    MyLogger.mylogger.info("Shutting down ReadLoadManager threadpool ....");
    try {
      threadPool.shutdown();
    } finally {
      mgrlatch.countDown();
    }
  }

}
