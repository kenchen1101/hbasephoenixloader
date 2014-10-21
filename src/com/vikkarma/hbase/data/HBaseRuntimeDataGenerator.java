package com.vikkarma.hbase.data;

import java.util.ArrayList;
import java.util.List;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseConstanats;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.RandomKeyUtils;

public class HBaseRuntimeDataGenerator {
	private static final HBaseTestProperties testProperties = HBaseTestProperties
			.getInstance();

	public static final int TOTAL_KEYS = SplitRowKeyGenerator.TOTAL_KEYS;
	private static final int INSERT_BULK = HBaseLoadParams.getWriteBatchSize();
	private static final int MAX_DATA_LENGTH = HBaseLoadParams.getRowKeyDataLen();
	public static final boolean SPLIT_TABLE = testProperties
			.getBoolean("SPLIT_TABLE");
	public static final boolean WAL_PUT = testProperties
			.getBoolean("WAL_PUT");

	//Realtime data generation loop counters
	private long univKeyCtr = 0;

	
	public HBaseRuntimeDataGenerator() {
		//Realtime data generation loop counters
		univKeyCtr = testProperties.getInteger("UNIV_KEY_CTR");
	}
	
	
	/**
	 * generate test data file, format given below
	 * <Rowkey=$ExchangeTopic_$Timestamp_$UniqueKey
	 * ><Column="HM">:<ColumnQualifier="HQ"><Value=$HistoryMessage>
	 */
	public List<HBaseRecord> generateSplitRegionHBasePutBatch(ETKey etkey) {

		//Generate data such that you have 1 bulk per ET from each region sequenced one after the other		
	  //MyLogger.mylogger.info("Starting batch generation for ETKey:"+ etkey);
		long beforetime = System.currentTimeMillis();
		List<HBaseRecord> putBatch = new ArrayList<HBaseRecord>();
		for (int key=0; key < INSERT_BULK; key++) {
			//3 = {"HistoryKey", "HistoryColumnQualifier","HistoryMessage"}
			String rowKey = generateRowKey(etkey.getXchgIndex(), etkey.getTopicIndex(), univKeyCtr++);
			String rowVal = RandomKeyUtils.getFixedString(MAX_DATA_LENGTH);
			HBaseRecord putRecord = new HBaseRecord(rowKey, HBaseConstanats.historyColumnQualifier, rowVal);
			putBatch.add(putRecord);
		}
		long optime = System.currentTimeMillis() - beforetime;
		/*
		MyLogger.mylogger.info("Put Batch Generation Time " + " StartTime=" + beforetime
				+ " OperationTime=" + optime + " put batch size = " + putBatch.size());
				*/
		return putBatch;
	}

	
	/**
	 * generate test data file, format given below
	 * <Rowkey=$ExchangeTopic_$Timestamp_$UniqueKey
	 * ><Column="HM">:<ColumnQualifier="HQ"><Value=$HistoryMessage>
	 */
	public List<HBaseRecord> generateSplitRegionHBaseGetBatch(ETKey etkey) {
		//Generate data such that you have 1 bulk per ET from each region sequenced one after the other		
		long beforetime = System.currentTimeMillis();
		List<HBaseRecord> getBatch = new ArrayList<HBaseRecord>();
		for (int key=0; key < INSERT_BULK; key++) {
			//3 = {"HistoryKey", "HistoryColumnQualifier","HistoryMessage"}
			String rowKey = generateRowKey(etkey.getXchgIndex(), etkey.getTopicIndex(), univKeyCtr++);
			HBaseRecord getRecord = new HBaseRecord(rowKey, "", "");
			getBatch.add(getRecord);
		}
		long optime = System.currentTimeMillis() - beforetime;
		MyLogger.mylogger.finer("Get Batch Generation Time " + " StartTime=" + beforetime
				+ " OperationTime=" + optime + " put batch size = " + getBatch.size());
		return getBatch;
	}
	
	//Generate formatted rowkey
	private String generateRowKey(int exchg, int topic, long univctr) {
		String rowkey_a = HBaseConstanats.exchangePrefix + exchg + ":"
				+ HBaseConstanats.topicPrefix + topic + ":";
		String rowkey = String.format("%s%08d", rowkey_a, univctr);
		return rowkey;
	}
	
}
