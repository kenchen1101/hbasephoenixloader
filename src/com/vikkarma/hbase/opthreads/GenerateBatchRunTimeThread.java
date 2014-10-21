package com.vikkarma.hbase.opthreads;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import com.vikkarma.hbase.data.ETKey;
import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.hbase.data.HBaseRuntimeDataGenerator;
import com.vikkarma.hbase.data.SplitRowKeyGenerator;
import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.MyLogger;

public class GenerateBatchRunTimeThread implements Runnable {
	private BlockingQueue<List<HBaseRecord>> blockingQueue;
	private final CountDownLatch opslatch;
	private static final int INSERT_BULK = HBaseLoadParams.getWriteBatchSize();
	private boolean readTest = false;
	private List<List<ETKey>> dataRegionIndexes = null;
	HBaseRuntimeDataGenerator runtimeDataGen = null;

	public GenerateBatchRunTimeThread(BlockingQueue<List<HBaseRecord>> blockingQueue,
			boolean readTest, CountDownLatch opslatch) {
		this.blockingQueue = blockingQueue;
		this.readTest = readTest;
		this.opslatch = opslatch;
		// Generate region index
		dataRegionIndexes = SplitRowKeyGenerator.getDataRegionIndex();
		runtimeDataGen = new HBaseRuntimeDataGenerator();

	}

	@Override
	public void run() {
		// Generate data such that you have 1 bulk per ET from each region
		// sequenced one after the other on a round robin basis
		try {
			int insertCounter = 0;
			for (int keys = 0; keys < SplitRowKeyGenerator.TOTAL_KEYS; keys = insertCounter) {
				for (int dataInSplitIndex = 0; dataInSplitIndex < SplitRowKeyGenerator.NUM_ETS_PER_SPLIT; dataInSplitIndex++) {
					for (int splitRegion = 0; splitRegion < SplitRowKeyGenerator.NUM_REGION_SPLITS; splitRegion++) {
						ETKey etkey = dataRegionIndexes.get(splitRegion).get(
								dataInSplitIndex);
						List<HBaseRecord> putGetBatch = null;
						if (readTest) {
							putGetBatch = runtimeDataGen
									.generateSplitRegionHBaseGetBatch(etkey);
						} else {
							putGetBatch = runtimeDataGen
									.generateSplitRegionHBasePutBatch(etkey);
						}
						// Put putGetBatch on the blocking Queue
						try {
							MyLogger.mylogger
									.info("InQueue : put batch of size: "
											+ putGetBatch.size());
							blockingQueue.put(putGetBatch);
							insertCounter = insertCounter + INSERT_BULK;
						} catch (InterruptedException e) {
							MyLogger.mylogger.log(Level.SEVERE,
									"Interrupted while adding a new batch", e.getMessage() + Arrays.toString(e.getStackTrace()));
						}
					}
				}
			}
		} finally {
			// Signal that all the put batch generation op is over
			opslatch.countDown();
		}

	}

}
