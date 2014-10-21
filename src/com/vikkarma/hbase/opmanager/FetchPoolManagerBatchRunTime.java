package com.vikkarma.hbase.opmanager;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.hbase.client.Row;

import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.hbase.opthreads.FetchScanRunTimeThread;
import com.vikkarma.hbase.opthreads.GenerateBatchRunTimeThread;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;

public class FetchPoolManagerBatchRunTime extends ReadLoadManager implements Runnable  {
	//Insert test properties
	private static final int INSERT_BULK = HBaseLoadParams.getWriteBatchSize();
	
	// Concurrency Modeling
	private static final HBaseTestProperties testProperties = HBaseTestProperties
			.getInstance();
	private static final int PUTGEN_HBASEIN_QUEUE_SIZE = testProperties
			.getInteger("PUTGEN_HBASEIN_QUEUE_SIZE"); //numHConn
	private static ThreadPoolExecutor hbaseBatchGeneratorPool;
	// Queue for the hbaseBatchGeneratorPool and hbaseWriterPool to exchange batches to write on HBase
	BlockingQueue<List<HBaseRecord>> blockingQueue = null;
	
	//Manager latches
	CountDownLatch batchGeneratorLatch = null;
	CountDownLatch batchReaderLatch = null;
	
	public FetchPoolManagerBatchRunTime(CountDownLatch cdl) {
	  super(cdl);
		//Generate put batches and insert into queue
		hbaseBatchGeneratorPool = (ThreadPoolExecutor) Executors
				.newFixedThreadPool(1); 
		blockingQueue = new ArrayBlockingQueue<List<HBaseRecord>>(PUTGEN_HBASEIN_QUEUE_SIZE);
	}
	
	/**
	 * Read the hbase test data file one bulk at a time 
	 * and insert it into hbase using a pool of 
	 * InsertHBaseThread 
	 */
	
	@Override	
	public void run() {
		// Start the put generator thread
		MyLogger.mylogger.info("Starting FetchPoolManagerBatchRunTime put batch generator thread ...");
		batchGeneratorLatch = new CountDownLatch(1);
		Thread readBatchKeyGenThread = new Thread(new GenerateBatchRunTimeThread(blockingQueue, true, batchGeneratorLatch));
		hbaseBatchGeneratorPool.execute(readBatchKeyGenThread);

		//Start the HBase writer threads
		MyLogger.mylogger.info("Starting FetchPoolManagerBatchRunTime ThreadPool Size[" + HBaseLoadParams.getNumWriters() +"] to read " + numPutBatches + " batches from HBase of size " + INSERT_BULK);
		batchReaderLatch = new CountDownLatch(numPutBatches);			
		long beforetime = System.currentTimeMillis();
		int threadCtr = 0;
		int fetchCounter = 0;
		//Generate data such that you have 1 bulk per ET from each region sequenced one after the other
		for (int keys=0; keys <SplitRowKeyGenerator.TOTAL_KEYS; keys = fetchCounter) {			
			for (int dataInSplitIndex=0;  dataInSplitIndex<SplitRowKeyGenerator.NUM_ETS_PER_SPLIT; dataInSplitIndex++) {
				for(int splitRegion=0; splitRegion < SplitRowKeyGenerator.NUM_REGION_SPLITS; splitRegion++) {
					MyLogger.mylogger.info("Creating batch fetch thread [" + threadCtr +"]");
					Thread thread = new Thread(new FetchScanRunTimeThread(blockingQueue, txnCtr, batchReaderLatch));
					threadPool.execute(thread);
					//Every thread fetches INSERT_BULK rowkeys in one batch
					threadCtr++;
					fetchCounter = fetchCounter + INSERT_BULK;
				}
			}
		}
		MyLogger.mylogger.info("Waiting for all threads["+numPutBatches+"] in FetchPoolManagerBatchRunTime to complete...");
    try {
      batchReaderLatch.await();
      //batchGeneratorLatch.await();
      //batchWriterLatch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      MyLogger.mylogger.severe("FetchPoolManagerBatchRunTime " + e);
    }
    //Shutdown the batch generator pool
    hbaseBatchGeneratorPool.shutdown();
		cleanUp(beforetime);
	}

}
