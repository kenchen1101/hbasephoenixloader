package com.vikkarma.hbase.opthreads;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.hbase.operations.HBaseStoreOperations;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.TransactionSuccessCounter;

public class FetchScanRunTimeThread implements Runnable {
  private BlockingQueue<List<HBaseRecord>> blockingQueue;
  private final CountDownLatch opslatch;
  private TransactionSuccessCounter txnCtr;
  private List<HBaseRecord> batch;

  public FetchScanRunTimeThread(BlockingQueue<List<HBaseRecord>> blockingQueue,
      TransactionSuccessCounter txnCtr, CountDownLatch opslatch) {
    this.blockingQueue = blockingQueue;
    this.txnCtr = txnCtr;
    this.opslatch = opslatch;
  }

  @Override
  public void run() {

    MyLogger.mylogger
        .finer("FetchScanRunTimeThread: Fetching batch from queue to insert into HBase");
    try {
      batch = blockingQueue.take();
    } catch (InterruptedException e1) {
      MyLogger.mylogger.severe("FetchScanRunTimeThread:" + e1);
      txnCtr.decrementCount();
    }

    MyLogger.mylogger.finer("FetchScanRunTimeThread: Fetch Batch from Hbase of size : "
        + batch.size());
    try {
      HBaseStoreOperations.fetchHBaseRecordDirectBatchScan(batch);
    } catch (Exception e) {
      MyLogger.mylogger.severe("FetchScanRunTimeThread: " + e.getMessage() + Arrays.toString(e.getStackTrace()));
      txnCtr.decrementCount();
    } finally {
      opslatch.countDown();
    }
  }

}
