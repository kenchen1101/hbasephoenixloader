package com.vikkarma.hbase.opmanager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.TransactionSuccessCounter;

public abstract class WriteLoadManager implements Runnable {
  private static final int ROW_VAL_SIZE = HBaseLoadParams.getRowKeyDataLen();
  protected static int numPutBatches = HBaseLoadParams.getNumXchg() * HBaseLoadParams.getNumTopic() * HBaseLoadParams.getNumKeysPerET()/HBaseLoadParams.getWriteBatchSize();
  protected static TransactionSuccessCounter txnCtr = new TransactionSuccessCounter(numPutBatches);

  protected static ThreadPoolExecutor threadPool;
  protected CountDownLatch mgrlatch;

  
  public WriteLoadManager(CountDownLatch cdl) {
    mgrlatch = cdl;
    threadPool = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(HBaseLoadParams.getNumWriters());
  }

  public void cleanUp(long beforetime) {
    long totalTime = System.currentTimeMillis() - beforetime;
    long writeTotalTput = (long) SplitRowKeyGenerator.TOTAL_KEYS / (long) totalTime;
    long writeTotalTputMBs = (long)((long)(ROW_VAL_SIZE + 53))*writeTotalTput/1024;
    MyLogger.mylogger.info("Expected:: Inserted into HBase " + SplitRowKeyGenerator.TOTAL_KEYS
        + " in " + numPutBatches + " batches. Total time required " + totalTime
        + " Throughput is " + writeTotalTput + " KRows/sec " + writeTotalTputMBs + " MB/sec");

    writeTotalTput =
        (long) HBaseLoadParams.getWriteBatchSize() * txnCtr.getCount() / (long) totalTime;
    writeTotalTputMBs = (long)((long)(ROW_VAL_SIZE + 53))*writeTotalTput/1024;
    MyLogger.mylogger.info("Actual:: Inserted into HBase " + HBaseLoadParams.getWriteBatchSize()
        * txnCtr.getCount() + " in " + txnCtr.getCount() + " batches. Total time required "
        + totalTime + " Throughput is " + writeTotalTput + " KRows/sec " + writeTotalTputMBs + " MB/sec");

    try {
      MyLogger.mylogger.info("Shutting down insert HBase insert manager threadpool ....");
      threadPool.shutdown();
    } finally {
      mgrlatch.countDown();
    }
  }
  
  public void cleanUp() {
    try {
      MyLogger.mylogger.info("Shutting down insert HBase insert manager threadpool ....");
      threadPool.shutdown();
    } finally {
      mgrlatch.countDown();
    }
  }
}
