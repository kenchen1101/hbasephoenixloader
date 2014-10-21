package com.vikkarma.hbase.operations;

import java.util.ArrayList;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseTestProperties;

public interface IHbaseOperations {

	static final HBaseTestProperties hbaseTestProperties = HBaseTestProperties
			.getInstance();
	static final String HBASE_HOST = hbaseTestProperties.get("HBASE_HOST");
	public static final String HBASE_PORT = hbaseTestProperties
			.get("HBASE_PORT");
	static final int COLUMN_RANGE_FILTER_BATCH_SIZE = hbaseTestProperties
			.getInteger("COLUMN_RANGE_FILTER_BATCH_SIZE");

	/** Maximum allowed pool size. */
//	static final int MAX_HTABLE_POOL_SIZE = hbaseTestProperties
//			.getInteger("MAX_HTABLE_POOL_SIZE");
	static final int MAX_HTABLE_POOL_SIZE = HBaseLoadParams.getNumHConn();

	/**
	 * Create a simple Hbase table
	 * 
	 * @param tableName
	 * @param familys
	 */
	public void createTable(String tableName, String[] familys);

	/**
	 * Create Table with Rowkey based Region splits
	 * 
	 * @param tableName
	 * @param familys
	 * @param regionsplits
	 */
	public void createSplitTable(String tableName, String[] familys,
			byte[][] regionsplits);

	/**
	 * Get number of split regions in a table
	 * 
	 * @param tableName
	 * @param startKey
	 * @param stopKey
	 * @return
	 */
	public int getHistoryTableRegions(String tableName, String startKey,
			String stopKey);

	/**
	 * Delete the table from Hbase
	 * 
	 * @param tableName
	 */
	public void deleteTable(String tableName);

	/**
	 * Put (or insert) a row
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value);

	/**
	 * Add a list of row key records to the table
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public void addRecordArray(String tableName, ArrayList<String> rowKey,
			String family, ArrayList<String> qualifier, ArrayList<String> value);

	/**
	 * get the table record for the given rowkey
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public void getRowKeyRecords(String tableName, String rowKey);

	/**
	 * Get the table record list for the given rowkey list
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public void getRowKeyRecordsArray(String tableName, ArrayList<String> rowKey);

	/**
	 * Get rows by scanner based on start and stop scan for rowkeys
	 * 
	 * @param tableName
	 * @param startscan
	 * @param endscan
	 */
	public void getRowKeyStartStopScanRecords(String tableName,
			String startscan, String endscan);

	/**
	 * Get records matching a rowkey prefix
	 * 
	 * @param tableName
	 * @param etkey
	 */
	public void getRowKeyPrefixFilteredRecords(String tableName, String etkey);

	/**
	 * Get records from a table with qualifiers with the given start and stop
	 * range
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param startQual
	 * @param endQual
	 */
	public void getQualifierFilteredRecords(String tableName, String rowKey,
			String startQual, String endQual);

	/**
	 * Get records from a table with start and stop range Column Filtering
	 * 
	 * @param tableName
	 * @param exchtopic
	 * @param startRange
	 * @param stopRange
	 */
	public void getColumnFilterRecords(String tableName, String exchtopic,
			String startRange, String stopRange);

	/**
	 * Get all records from a table To be used only for small tables
	 * 
	 * @param tableName
	 */
	public void getAllRecord(String tableName);

	/**
	 * Delete entry from Hbase table based on rowkey
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public void delRecord(String tableName, String rowKey);

	/**
	 * Delete list of entries from Hbase table based on rowkey
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public void delRecordArray(String tableName, ArrayList<String> rowKey);
}
