package com.vikkarma.hbase.connector;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseTestProperties;

public class HbaseConnectionParams {
	
	//Hbase Property File Reader
	private static final HBaseTestProperties hbaseTestProperties = HBaseTestProperties
			.getInstance();

	//Hbase Connection params
	private static final String HBASE_MASTER_HOST = hbaseTestProperties.get("MULTI_HBASE_HOST");
	public static final String HBASE_PORT = hbaseTestProperties
			.get("HBASE_PORT");	

//	private static final int MAX_HTABLE_POOL_SIZE = hbaseTestProperties
//			.getInteger("MAX_HTABLE_POOL_SIZE");
	private static final int MAX_HTABLE_POOL_SIZE = HBaseLoadParams.getNumHConn();
	
	//If replication is enabled all operations will always occur on primary cluster only
	private static final boolean IS_REPLICATION_ENABLED = hbaseTestProperties.getBoolean("IS_REPLICATION_ENABLED");
	
	public static String getHbaseMasterHost() {
		return HBASE_MASTER_HOST;
	}
	public static String getHbasePort() {
		return HBASE_PORT;
	}
	public static int getMaxHtablePoolSize() {
		return MAX_HTABLE_POOL_SIZE;
	}
	
	/**
	 * 0 for primary HBase instance and 1 for DR HBase instance
	 * @return
	 */
	public static int getPrimaryHBaseInstanceIndex() {
		return 0;
	}
	public static int getDRHBaseInstanceIndex() {
		return 1;
	}

}
