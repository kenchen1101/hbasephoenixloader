package com.vikkarma.hbase.data;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import au.com.bytecode.opencsv.CSVReader;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseTestProperties;
import com.vikkarma.utils.MyLogger;
import com.vikkarma.utils.RandomKeyUtils;

public class LoadHBaseDataFile {
	
	private static final HBaseTestProperties testProperties = HBaseTestProperties.getInstance();	
	private static final String historyDataFile=testProperties.get("HISTORY_FILE_NAME");
	private static final boolean GENERATE_KEYS_ONLY  = testProperties.getBoolean("GENERATE_KEYS_ONLY");
	private static final boolean GENERATE_FIXED_SIZE_DATA  = testProperties.getBoolean("GENERATE_FIXED_SIZE_DATA");	
	private static final int MAX_DATA_LENGTH = HBaseLoadParams.getRowKeyDataLen();
	
	private List<HBaseRecord> hbaseDataChunk; 
	private static CSVReader reader=null;
	private static String startKey=null;
	private static String endKey=null;
	private Random randomGenerator;
	

	/**
	 * Initialize file handler
	 */
	public LoadHBaseDataFile() {
    MyLogger.mylogger.fine("Reading History data from " + historyDataFile + " ... ");
	  randomGenerator = new Random();
		try {
			reader = new CSVReader(new FileReader(historyDataFile));
			//Ignore first line (header) from input csv file
			try {
				reader.readNext();
			} catch (IOException e) {
				MyLogger.mylogger.severe("LoadHBaseDataFile " + e.getMessage() + Arrays.toString(e.getStackTrace()));
			}
		} catch (FileNotFoundException e) {
			MyLogger.mylogger.severe("LoadHBaseDataFile " + e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
		
	}

	/**
	 * Read entire file
	 * @return
	 */
	public int readAllHistoryDataFile() {
		hbaseDataChunk = new ArrayList<HBaseRecord>(); 
		String [] nextLine;		
		try {
			while ((nextLine = reader.readNext()) != null) {				
				populateHistoryData(nextLine);
			}
			
		} catch (IOException e) {	
			MyLogger.mylogger.severe("readAllHistoryDataFile " + e.getMessage() + Arrays.toString(e.getStackTrace()));
		}
		return hbaseDataChunk.size();
	}

	/**
	 * Read in bulk for huge files
	 * @param bulksize
	 * @return
	 */
	public int readBulkHistoryDataFile(int bulksize) {	
		hbaseDataChunk = new ArrayList<HBaseRecord>(); 

		String [] nextLine;	
		for (int i=0;i<bulksize; i++) {
			try {
				if ((nextLine = reader.readNext()) == null) {
					return hbaseDataChunk.size();
				}
				populateHistoryData(nextLine);
			} catch (IOException e) {
				MyLogger.mylogger.severe("readBulkHistoryDataFile " + e.getMessage() + Arrays.toString(e.getStackTrace()));
				//e.printStackTrace();
			}
		}
		return hbaseDataChunk.size();

	}
	
	/**
	 * Populate core object data structure
	 * @param nextLine
	 */
	private void populateHistoryData(String[] nextLine) {
		//store StartKey and endKey for RegionInfo
		if (startKey==null) {
			startKey=nextLine[0];
		}
		endKey=nextLine[0];
		
		HBaseRecord hrecord;
		//if data file contains only keys generate and populate random data
		if (GENERATE_KEYS_ONLY && !GENERATE_FIXED_SIZE_DATA) {
			String randomData = RandomKeyUtils.getRandomString(randomGenerator.nextInt(MAX_DATA_LENGTH));
			hrecord = new HBaseRecord(nextLine[0], nextLine[1], randomData);
		} else if (GENERATE_KEYS_ONLY && GENERATE_FIXED_SIZE_DATA) {
			String fixedData = RandomKeyUtils.getFixedString(MAX_DATA_LENGTH);
			hrecord = new HBaseRecord(nextLine[0], nextLine[1], fixedData);
			
		} else {
			hrecord = new HBaseRecord(nextLine[0], nextLine[1], nextLine[2]);
		}
		hbaseDataChunk.add(hrecord);

	}
	
	public List<HBaseRecord> getHbaseDataChunk() {
		return hbaseDataChunk;
	}
	
	public static String getStartKey() {
		return startKey;
	}
	public static String getEndKey() {
		return endKey;
	}
	
	public ArrayList<String> getKeyList() {
		ArrayList<String> keyList = new ArrayList<String>();
		for (int i=0;i<hbaseDataChunk.size();i++) {
			keyList.add(hbaseDataChunk.get(i).getKey());
		}
		return keyList;
	}
	
	/**
	 * Close data file
	 */
	public void closecsv() {
		if(reader != null ) {
			try {
				reader.close();
			} catch (IOException e) {
				MyLogger.mylogger.severe("closecsv " + e.getMessage() + Arrays.toString(e.getStackTrace()));
				//e.printStackTrace();
			}
		}
	}

}
