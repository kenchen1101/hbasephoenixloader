package com.vikkarma.hbase.opmanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.hbase.opthreads.GenerateBatchRunTimeThread;
import com.vikkarma.hbase.opthreads.InsertBatchRunTimeThread;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.PhoenixUtils;

public class InsertPoolManagerBatchRunTime extends WriteLoadManager implements Runnable {
  // Insert test properties
  private static final int INSERT_BULK = HBaseLoadParams.getWriteBatchSize();

  // Concurrency Modeling
  private static final HBaseTestProperties testProperties = HBaseTestProperties.getInstance();  
  private static final int PUTGEN_HBASEIN_QUEUE_SIZE = testProperties
      .getInteger("PUTGEN_HBASEIN_QUEUE_SIZE"); // numHConn
  private static ThreadPoolExecutor hbaseBatchGeneratorPool;
  // Queue for the hbaseBatchGeneratorPool and hbaseWriterPool to exchange batches to write on HBase
  BlockingQueue<List<HBaseRecord>> blockingQueue = null;
  private static final int PS_QUERY_TIMEOUT = testProperties
      .getInteger("PS_QUERY_TIMEOUT");

  // Manager latches
  CountDownLatch batchGeneratorLatch = null;
  CountDownLatch batchWriterLatch = null;

  public InsertPoolManagerBatchRunTime(CountDownLatch cdl) {
    super(cdl);
    // Generate put batches and insert into queue
    hbaseBatchGeneratorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    blockingQueue = new ArrayBlockingQueue<List<HBaseRecord>>(PUTGEN_HBASEIN_QUEUE_SIZE);
  }

  /**
   * Read the hbase test data file one bulk at a time and insert it into hbase using a pool of
   * InsertHBaseThread
   */

  @Override
  public void run() {
    // Start the put generator thread
    MyLogger.mylogger.info("Starting InsertPoolManagerBatchRunTime put batch generator thread ...");
    batchGeneratorLatch = new CountDownLatch(1);
    hbaseBatchGeneratorPool.execute(new GenerateBatchRunTimeThread(blockingQueue, false,
        batchGeneratorLatch));

    // Start the HBase writer threads
    MyLogger.mylogger.info("Starting InsertPoolManagerBatchRunTime ThreadPool Size["
        + HBaseLoadParams.getNumWriters() + "] to write " + numPutBatches
        + " batches to HBase of size " + HBaseLoadParams.getWriteBatchSize());
    batchWriterLatch = new CountDownLatch(numPutBatches);
    long beforetime = System.currentTimeMillis();
    int batchCtr = 0;
    int insertCounter = 0;
    // Generate data such that you have 1 bulk per ET from each region sequenced one after the other
    final List<Future<?>> list = new ArrayList<>();
    for (int keys = 0; keys < SplitRowKeyGenerator.TOTAL_KEYS; keys = insertCounter) {
      for (int dataInSplitIndex = 0; dataInSplitIndex < SplitRowKeyGenerator.NUM_ETS_PER_SPLIT; dataInSplitIndex++) {
        for (int splitRegion = 0; splitRegion < SplitRowKeyGenerator.NUM_REGION_SPLITS; splitRegion++) {
          if(batchCtr%100 == 0) {
            MyLogger.mylogger.info("Picking up batch [" + batchCtr + "] from the queue to insert into HBase cluster");
          }
          //threadPool.execute(new InsertBatchRunTimeThread(blockingQueue, txnCtr, batchWriterLatch));
          final Future<?> future = threadPool.submit(new InsertBatchRunTimeThread(blockingQueue, txnCtr, batchWriterLatch));
          list.add(future);
          // Every batch inserts INSERT_BULK rowkeys
          batchCtr++;
          insertCounter = insertCounter + INSERT_BULK;
        }
      }
    }

    MyLogger.mylogger.info("Waiting for all batches[" + numPutBatches
      + "] in executor queue to complete...");

    //Wait till latchcount stops decrementing  
    long previousLatchCount=batchWriterLatch.getCount();
    long currentLatchCount=0;
    long notChangingLatchCount=0;
    while (true) {
      try {
        MyLogger.mylogger.info("Sleeping for 5 seconds for total batches[" + numPutBatches
          + "] to complete. Current count="+ batchWriterLatch.getCount());
        Thread.sleep(5000);
      } catch (InterruptedException e) {        
        MyLogger.mylogger.severe("Sleep interrupted: "+ e.getMessage() + Arrays.toString(e.getStackTrace()));
      }
      currentLatchCount=batchWriterLatch.getCount();
      if (currentLatchCount == 0 ){
    	  break;
      }
      if(currentLatchCount==previousLatchCount) {
        notChangingLatchCount++;
      } else {
        notChangingLatchCount=0;
      }
      if(notChangingLatchCount==5) {
        MyLogger.mylogger.info("Exiting since latch count is not changing, looks like tasks are hung. Current count="+ batchWriterLatch.getCount());
        break;
      }
      previousLatchCount=currentLatchCount;
    }
    //Once Latches are detected to not update, timeout and kill the remaining batches 
    PhoenixUtils.waitForTask(list, PS_QUERY_TIMEOUT, TimeUnit.SECONDS);
    
    //Finally wait for 10 seconds and clear the latch
    try {
      //batchWriterLatch.await();
      // batchGeneratorLatch.await();
      batchWriterLatch.await(PS_QUERY_TIMEOUT, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      MyLogger.mylogger.log(Level.SEVERE, "Interrupted waiting for batches to complete", e.getMessage() + Arrays.toString(e.getStackTrace()));
    }
    //Shutdown the batch generator pool
    hbaseBatchGeneratorPool.shutdown();
    cleanUp(beforetime);
  }
}
