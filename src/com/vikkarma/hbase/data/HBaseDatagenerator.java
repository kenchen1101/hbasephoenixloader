package com.vikkarma.hbase.data;


import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.HBaseConstanats;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.RandomKeyUtils;

import au.com.bytecode.opencsv.CSVWriter;

public class HBaseDatagenerator {
	private static final HBaseTestProperties testProperties = HBaseTestProperties
			.getInstance();
	private static final String HISTORY_FILE_NAME = testProperties
			.get("HISTORY_FILE_NAME");
	public static final int TOTAL_KEYS = SplitRowKeyGenerator.TOTAL_KEYS;
	private static final int INSERT_BULK = HBaseLoadParams.getWriteBatchSize();
	private static final boolean GENERATE_KEYS_ONLY = testProperties
			.getBoolean("GENERATE_KEYS_ONLY");
	private static final boolean RUNTIME_BATCH_GEN = testProperties
			.getBoolean("RUNTIME_BATCH_GEN");
	private static final boolean GENERATE_FIXED_SIZE_DATA = testProperties
			.getBoolean("GENERATE_FIXED_SIZE_DATA");
	private static final int MAX_DATA_LENGTH = HBaseLoadParams
			.getRowKeyDataLen();
	public static final boolean SPLIT_TABLE = testProperties
			.getBoolean("SPLIT_TABLE");

	private static CSVWriter testDataFileWriter = null;

	// private static final String[] historyColumnQualifierHeader =
	// {"HistoryKey", "HistoryColumnQualifier","HistoryMessage"};
	private static final String[] hbaseFileHeader = { "HK", "HQ", "HM" };

	/**
	 * Initialize test data file writer
	 */
	public HBaseDatagenerator() {
		try {
			testDataFileWriter = new CSVWriter(
					new FileWriter(HISTORY_FILE_NAME));
			testDataFileWriter.writeNext(hbaseFileHeader);
		} catch (IOException e) {
			MyLogger.mylogger.severe("Exception caught in HBaseDatagenerator "
					+ e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
	}

	private String generateRowKey(int exchg, int topic, int univctr) {
		String rowkey_a = HBaseConstanats.exchangePrefix + exchg + ":"
				+ HBaseConstanats.topicPrefix + topic + ":";
		//Runtime batch generation shd be fast, hence no need for timestamped key
		String rowkey = null;
		if (RUNTIME_BATCH_GEN) {
			rowkey = String.format("%s%08d" ,rowkey_a, univctr);
		} else {
	    String rowkey_b = ":" + System.currentTimeMillis();
			rowkey = String.format("%s%08d%s" ,rowkey_a, univctr, rowkey_b);
		}
		return rowkey;
	}

	/**
	 * generate test data file, format given below
	 * <Rowkey=$ExchangeTopic_$Timestamp_$UniqueKey
	 * ><Column="HM">:<ColumnQualifier="HQ"><Value=$HistoryMessage>
	 */
	public void generateSplitBatchHbaseDataFiles() {
		long beforetime = System.currentTimeMillis();
		// Generate table split key file
		SplitRowKeyGenerator.generateSplitKeysFile();

		if (!RUNTIME_BATCH_GEN) {
			List<List<ETKey>> dataRegionIndexes = SplitRowKeyGenerator
					.getDataRegionIndex();
			// Generate data such that you have 1 bulk per ET from each region
			// sequenced one after the other
			int univKeyCtr = 0;
			int keysInserted = 0;
			for (int keys = 0; keys < TOTAL_KEYS; keys = keysInserted) {
				MyLogger.mylogger.info("SplitKey Info 1: Generating keys upto "
						+ keys + " of TOTAL_KEYS " + TOTAL_KEYS);
				for (int dataInSplitIndex = 0; dataInSplitIndex < SplitRowKeyGenerator.NUM_ETS_PER_SPLIT; dataInSplitIndex++) {
					// MyLogger.mylogger.info("SplitKey Info 2: Generating dataInSplitIndex from 0 to "
					// + SplitRowKeyGenerator.NUM_ETS_PER_SPLIT);
					for (int splitRegion = 0; splitRegion < SplitRowKeyGenerator.NUM_REGION_SPLITS; splitRegion++) {
						// MyLogger.mylogger.info("SplitKey Info 3: splitRegion from 0 to "
						// + SplitRowKeyGenerator.NUM_REGION_SPLITS);
						ETKey etkey = dataRegionIndexes.get(splitRegion).get(
								dataInSplitIndex);
						// MyLogger.mylogger.info("SplitKey Info 4: splitRegion "
						// + splitRegion + " dataInSplitIndex " +
						// dataInSplitIndex);
						for (int key = 0; key < INSERT_BULK; key++) {
							// MyLogger.mylogger.info("SplitKey Info 5: key from 0 to "
							// + INSERT_BULK);
							String[] userData = new String[hbaseFileHeader.length];
							userData[0] = generateRowKey(etkey.getXchgIndex(),
									etkey.getTopicIndex(), univKeyCtr++);
							userData[1] = hbaseFileHeader[1];
							// If generate keys only do not populate data
							// random data values will be generated at the time
							// of insertion
							if (!GENERATE_KEYS_ONLY) {
								userData[2] = RandomKeyUtils
										.getFixedString(MAX_DATA_LENGTH);
							}
							testDataFileWriter.writeNext(userData);
							keysInserted++;
						}
					}
				}
				MyLogger.mylogger.severe("SplitKey Info 1: keysInserted "
						+ keysInserted);
			}
		}
		long optime = System.currentTimeMillis() - beforetime;
		MyLogger.mylogger.finer("HBase Data File generation : Action="
				+ "generateSplitBatchHbaseDataFiles" + " StartTime="
				+ beforetime + " OperationTime=" + optime);

		printDataSummary();
		// Close the data File
		try {
			testDataFileWriter.close();
		} catch (IOException e) {
			MyLogger.mylogger
					.severe("Exception caught in generateHbaseDataFiles " + e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
	}

	/**
	 * generate test data file, format given below
	 * <Rowkey=$ExchangeTopic_$Timestamp_$UniqueKey
	 * ><Column="HM">:<ColumnQualifier="HQ"><Value=$HistoryMessage>
	 */
	public void generateHbaseDataFiles() {
		int univKeyCtr = 0;
		Random randomGenerator = new Random();
		for (int exchg = 0; exchg < SplitRowKeyGenerator.NUM_EXCHANGE; exchg++) {
			for (int topic = 0; topic < SplitRowKeyGenerator.NUM_TOPICS; topic++) {
				for (int key = 0; key < SplitRowKeyGenerator.NUM_KEYS_PER_ET; key++) {
					String[] userData = new String[hbaseFileHeader.length];
					userData[0] = generateRowKey(exchg, topic, univKeyCtr++);
					userData[1] = hbaseFileHeader[1];
					// If generate keys only do not populate data
					// random data values will be generated at the time of
					// insertion
					if (!GENERATE_KEYS_ONLY && !GENERATE_FIXED_SIZE_DATA) {
						userData[2] = RandomKeyUtils
								.getRandomString(randomGenerator
										.nextInt(MAX_DATA_LENGTH));
					} else if (!GENERATE_KEYS_ONLY && GENERATE_FIXED_SIZE_DATA) {
						userData[2] = RandomKeyUtils
								.getFixedString(MAX_DATA_LENGTH);
					}
					testDataFileWriter.writeNext(userData);
				}
			}
		}

		printDataSummary();
		// Close the data File
		try {
			testDataFileWriter.close();
		} catch (IOException e) {
			MyLogger.mylogger
					.severe("Exception caught in generateHbaseDataFiles " + e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
	}

	/**
	 * generate test data file, format given below
	 * <Rowkey=$ExchangeTopic_$Timestamp_$UniqueKey
	 * ><Column="HM">:<ColumnQualifier="HQ"><Value=$HistoryMessage>
	 */
	public void generateSplitTableHbaseDataFiles() {
		int univKeyCtr = 0;
		Random randomGenerator = new Random();
		for (int key = 0; key < SplitRowKeyGenerator.NUM_KEYS_PER_ET; key++) {
			for (int topic = 0; topic < SplitRowKeyGenerator.NUM_TOPICS; topic++) {
				for (int exchg = 0; exchg < SplitRowKeyGenerator.NUM_EXCHANGE; exchg++) {
					String[] userData = new String[hbaseFileHeader.length];
					userData[0] = generateRowKey(exchg, topic, univKeyCtr++);
					userData[1] = hbaseFileHeader[1];
					// If generate keys only do not populate data
					// random data values will be generated at the time of
					// insertion
					if (!GENERATE_KEYS_ONLY) {
						userData[2] = RandomKeyUtils
								.getRandomString(randomGenerator
										.nextInt(MAX_DATA_LENGTH));
					}
					testDataFileWriter.writeNext(userData);

				}
			}
		}

		// Generate table split key file
		SplitRowKeyGenerator.generateSplitKeysFile();

		printDataSummary();
		// Close the data File
		try {
			testDataFileWriter.close();
		} catch (IOException e) {
			MyLogger.mylogger
					.severe("Exception caught in generateHbaseDataFiles " + e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
	}

	/**
	 * generate test data file, format given below
	 * <Rowkey=$ExchangeTopic><Column
	 * ="HM">:<ColumnQualifier=$UniqueKey_$Timestamp><Value=$HistoryMessage>
	 */
	public void generateColumnQualifierHbaseDataFiles() {
		int univKeyCtr = 0;
		Random randomGenerator = new Random();
		for (int exchg = 0; exchg < SplitRowKeyGenerator.NUM_EXCHANGE; exchg++) {
			for (int topic = 0; topic < SplitRowKeyGenerator.NUM_TOPICS; topic++) {
				for (int key = 0; key < SplitRowKeyGenerator.NUM_KEYS_PER_ET; key++) {
					String[] userData = new String[hbaseFileHeader.length];
					userData[0] = HBaseConstanats.exchangePrefix + exchg + ":"
							+ HBaseConstanats.topicPrefix + topic;
					String univKeyCtrString = String.format("%09d", univKeyCtr);
					userData[1] = univKeyCtrString + "_"
							+ System.currentTimeMillis();
					// userData[1] = univKeyCtrString;
					if (!GENERATE_KEYS_ONLY) {
						userData[2] = RandomKeyUtils
								.getRandomString(randomGenerator
										.nextInt(MAX_DATA_LENGTH));
					} else {
						userData[2] = "a";
					}
					testDataFileWriter.writeNext(userData);
					univKeyCtr++;
				}
			}
		}

		printDataSummary();
		// Close the data File
		try {
			testDataFileWriter.close();
		} catch (IOException e) {
			MyLogger.mylogger
					.severe("Exception caught in generateColumnQualifierHbaseDataFiles "
							+ e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
	}

	/**
	 * Print summary info abt the data generated for HBase tests
	 */
	private void printDataSummary() {
		StringBuilder str = new StringBuilder("Generated HBase data summary \n");
		str.append("Generated " + SplitRowKeyGenerator.NUM_EXCHANGE
				+ " Exchanges ..\n");
		str.append("Generated " + SplitRowKeyGenerator.NUM_EXCHANGE
				* SplitRowKeyGenerator.NUM_TOPICS + " Topics ..\n");
		str.append("Generated " + SplitRowKeyGenerator.NUM_EXCHANGE
				* SplitRowKeyGenerator.NUM_TOPICS
				* SplitRowKeyGenerator.NUM_KEYS_PER_ET + " Keys ..\n");
		str.append("Data File sgdata/" + HISTORY_FILE_NAME + "\n");
		String summary = str.toString();
		System.out.println(summary);
		MyLogger.mylogger.info(summary);
	}

	/**
	 * Read the test data file and find the start and stop keys for the number
	 * of table splits to be generated
	 * 
	 * @param numRegions
	 * @return
	 */
	public byte[][] getRegionSplitsFromDataFile() {
		byte[][] splitKeys = new byte[SplitRowKeyGenerator.NUM_REGION_SPLITS][];
		int regionSize = SplitRowKeyGenerator.NUM_EXCHANGE
				* SplitRowKeyGenerator.NUM_TOPICS
				* SplitRowKeyGenerator.NUM_KEYS_PER_ET
				/ SplitRowKeyGenerator.NUM_REGION_SPLITS;

		LoadHBaseDataFile histData = new LoadHBaseDataFile();
		int numNodeOps = 10;
		int regionIndex = 0;
		while (numNodeOps != 0) {
			// read graph data and initialize DB generator
			numNodeOps = histData.readBulkHistoryDataFile(regionSize);

			if (numNodeOps > 0) {
				// String startRegionKey =
				// histData.historydatachunk.get(0).getKey();
				String endRegionKey = histData.getHbaseDataChunk()
						.get(histData.getHbaseDataChunk().size() - 1).getKey();
				byte[] b = endRegionKey.getBytes();
				splitKeys[regionIndex] = b;
				regionIndex++;
			}
		}

		histData.closecsv();
		return splitKeys;
	}

}
