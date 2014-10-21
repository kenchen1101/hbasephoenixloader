package com.vikkarma.hbase.opthreads;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.hbase.operations.HBaseStoreOperations;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.TransactionSuccessCounter;

public class InsertBatchRunTimeThread implements Runnable {
  private BlockingQueue<List<HBaseRecord>> blockingQueue;
  private TransactionSuccessCounter txnCtr;
  private final CountDownLatch opslatch;
  private List<HBaseRecord> batch;

  public InsertBatchRunTimeThread(BlockingQueue<List<HBaseRecord>> blockingQueue,
      TransactionSuccessCounter txnCtr, CountDownLatch opslatch) {
    this.blockingQueue = blockingQueue;
    this.txnCtr = txnCtr;
    this.opslatch = opslatch;
  }

  @Override
  public void run() {

    MyLogger.mylogger
        .finer("InsertBatchRunTimeThread: Fetching batch from queue to insert into HBase");
    try {
      batch = blockingQueue.take();
    } catch (InterruptedException e) {
      MyLogger.mylogger.severe("InsertBatchRunTimeThread:" + e.getMessage() + Arrays.toString(e.getStackTrace()));
    }

    MyLogger.mylogger.finer("InsertBatchRunTimeThread: Insert Batch Into Hbase of size : "
        + batch.size());
    try {
      HBaseStoreOperations.addHBaseRecordDirectBatch(batch);
    } catch (Exception e) {
      MyLogger.mylogger.severe("InsertBatchRunTimeThread: " + e.getMessage() + Arrays.toString(e.getStackTrace()));
      e.printStackTrace();
      txnCtr.decrementCount();
    } finally {
      opslatch.countDown();
    }
  }

}
