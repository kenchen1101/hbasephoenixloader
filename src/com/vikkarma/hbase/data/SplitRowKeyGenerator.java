package com.vikkarma.hbase.data;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.HBaseConstanats;
import com.vikkarma.utils.MyLogger;

public class SplitRowKeyGenerator {

	private static final HBaseTestProperties testProperties = HBaseTestProperties
			.getInstance();
	public static final boolean SPLIT_TABLE = testProperties
			.getBoolean("SPLIT_TABLE");
	public static final int NUM_REGION_SPLITS = testProperties
			.getInteger("NUM_REGION_SPLITS");
	public static final int NUM_EXCHANGE = HBaseLoadParams.getNumXchg();
	public static final int NUM_TOPICS = HBaseLoadParams.getNumTopic();
	public static final int NUM_KEYS_PER_ET = HBaseLoadParams.getNumKeysPerET();
	public static final int TOTAL_KEYS = NUM_EXCHANGE * NUM_TOPICS * NUM_KEYS_PER_ET;
	
	public static final int NUM_KEYS_PER_SPLIT = NUM_EXCHANGE * NUM_TOPICS
			* NUM_KEYS_PER_ET / NUM_REGION_SPLITS;
	public static final int NUM_ETS_PER_SPLIT = NUM_EXCHANGE * NUM_TOPICS
			/ NUM_REGION_SPLITS;

	private static final boolean RUNTIME_BATCH_GEN = testProperties
			.getBoolean("RUNTIME_BATCH_GEN");
	private static byte[][] splitKeys = new byte[NUM_REGION_SPLITS][];
	private static List<List<ETKey>> dataRegionIndex = new ArrayList<List<ETKey>>();


	private static int splitIndex = 0;

	private static final String KEY_FILE_NAME = testProperties
			.get("KEY_FILE_NAME");
	private static CSVWriter keyFileWriter = null;

	/**
	 * Generate approximate key values to be used for table splitting
	 */
	public static void generateSplitKeys() {
		int splitCtr = 0;
		int univKeyCtr = 0;
		List<ETKey> regionSplitIndex = new ArrayList<ETKey>();
		for (int exchg = 0; exchg < NUM_EXCHANGE; exchg++) {
			for (int topic = 0; topic < NUM_TOPICS; topic++) {
				ETKey etIndex = new ETKey(exchg, topic);
				regionSplitIndex.add(etIndex);
				for (int key = 0; key < NUM_KEYS_PER_ET; key++) {
					splitCtr++;
					univKeyCtr++;
					if (splitCtr == NUM_KEYS_PER_SPLIT) {
						String splitKey = HBaseConstanats.exchangePrefix
								+ exchg + ":" + HBaseConstanats.topicPrefix
								+ topic + ":" + univKeyCtr++ + ":0000000000000";
						addSplitKeys(splitKey);
						dataRegionIndex.add(regionSplitIndex);
						regionSplitIndex = new ArrayList<ETKey>();
						splitCtr = 0;
					}

				}
			}
		}
	}

	/**
	 * Generate approximate key values to be used for table splitting
	 */
	public static void generateETSplitKeys() {
		MyLogger.mylogger.info("SplitKey Info: NUM_EXCHANGE: " + NUM_EXCHANGE + " NUM_TOPICS:" + NUM_TOPICS + " NUM_ETS_PER_SPLIT:" + NUM_ETS_PER_SPLIT);
		MyLogger.mylogger.info("SplitKey Info: TOTAL_KEYS: " + TOTAL_KEYS + " NUM_KEYS_PER_SPLIT:" + NUM_KEYS_PER_SPLIT);
		int splitCtr = 0;
		int univKeyCtr = 0;
		List<ETKey> regionSplitIndex = new ArrayList<ETKey>();
		for (int exchg = 0; exchg < NUM_EXCHANGE; exchg++) {
			for (int topic = 0; topic < NUM_TOPICS; topic++) {
				ETKey etIndex = new ETKey(exchg, topic);
				regionSplitIndex.add(etIndex);
				splitCtr++;
				if (splitCtr == NUM_ETS_PER_SPLIT) {
					String splitKey_a = HBaseConstanats.exchangePrefix + exchg
							+ ":" + HBaseConstanats.topicPrefix + topic + ":";
					//Runtime batch generation shd be fast, hence no need for timestamped key
					String splitKey = null;
					if (RUNTIME_BATCH_GEN) {
						splitKey = String.format("%s%08d" ,splitKey_a, univKeyCtr);
					} else {
						String splitKey_b = ":0000000000000";
						splitKey = String.format("%s%08d%s" ,splitKey_a, univKeyCtr, splitKey_b);
					}
					addSplitKeys(splitKey);
					//MyLogger.mylogger.info("SplitKey Info: Splitting on " + splitKey);
					dataRegionIndex.add(regionSplitIndex);
					//MyLogger.mylogger.info("SplitKey Info: regionSplitIndex " + regionSplitIndex.size());
					regionSplitIndex = new ArrayList<ETKey>();
					splitCtr = 0;
				}

			}
			
		}
		MyLogger.mylogger.info("SplitKey Info: dataRegionIndex " + dataRegionIndex.size());
	}

	/**
	 * keep an inmemory copy of the split keys
	 * 
	 * @param key
	 */
	public static void addSplitKeys(String key) {
		byte[] b = key.getBytes();
		splitKeys[splitIndex] = b;
		splitIndex++;
	}

	/**
	 * Write all the table split rowkeys to a key file
	 */
	public static void generateSplitKeysFile() {
		// calculate the value of table split keys
		//generateSplitKeys();
		generateETSplitKeys();

		// dump the keys in a file
		try {
			keyFileWriter = new CSVWriter(new FileWriter(KEY_FILE_NAME));
		} catch (IOException e) {
			MyLogger.mylogger
					.severe("Exception caught in SplitRowKeyGenerator " + e.getMessage() + Arrays.toString(e.getStackTrace()));
			// e.printStackTrace();
		}

		for (int i = 0; i < splitKeys.length; i++) {
			String[] keyArr = new String[1];
			keyArr[0] = new String(splitKeys[i]);
			keyFileWriter.writeNext(keyArr);
		}

		// Close the data File
		try {
			keyFileWriter.close();
		} catch (IOException e) {
			MyLogger.mylogger
					.severe("Exception caught in SplitRowKeyGenerator " + e.getMessage() + Arrays.toString(e.getStackTrace()));
			// e.printStackTrace();
		}
		System.out.println("Key File sgdata/" + KEY_FILE_NAME + "\n");
	}

	/**
	 * Return the keys stored in key file as an array for identifying table
	 * splits
	 * 
	 * @return
	 */
	public static String[] readKeyFile() {
		MyLogger.mylogger
				.info("Reading table split keys from " + KEY_FILE_NAME);
		String[] keyArr = new String[NUM_REGION_SPLITS];
		int keyArrIndex = 0;
		String[] nextLine;
		try {
			CSVReader keyreader = new CSVReader(new FileReader(KEY_FILE_NAME));
			while ((nextLine = keyreader.readNext()) != null) {
				// regex not working check later
				keyArr[keyArrIndex] = nextLine[0].replaceAll("\"", "");
				keyArrIndex++;
			}
			keyreader.close();
		} catch (FileNotFoundException e) {
			MyLogger.mylogger.severe("Exception caught in readKeyFile " + e.getMessage() + Arrays.toString(e.getStackTrace()));
			// e.printStackTrace();
		} catch (IOException e) {
			MyLogger.mylogger.severe("Exception caught in readKeyFile " + e.getMessage() + Arrays.toString(e.getStackTrace()));
			// e.printStackTrace();
		}
		return keyArr;
	}

	/**
	 * Read the key file generated with the data file and get the keys to split
	 * the table
	 * 
	 * @param numRegions
	 * @return
	 */
	public static byte[][] getRegionSplitsFromKeyFile() {
		String[] keyArr = readKeyFile();

		byte[][] splitKeys = new byte[SplitRowKeyGenerator.NUM_REGION_SPLITS][];
		for (int i = 0; i < keyArr.length; i++) {
			byte[] b = keyArr[i].getBytes();
			splitKeys[i] = b;
		}
		return splitKeys;
	}

	public static byte[][] getSplitKeys() {
		return splitKeys;
	}
	
	public static List<List<ETKey>> getDataRegionIndex() {
		return dataRegionIndex;
	}

}
