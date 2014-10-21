package com.vikkarma.hbase.operations;

import java.util.List;

import com.vikkarma.hbase.data.HBaseRecord;
import com.vikkarma.utils.HBaseTestProperties;

public class HBaseStoreOperations {
	private static final HBaseTestProperties gdbTestProperties = HBaseTestProperties.getInstance();	
	private static final String TABLE_NAME = gdbTestProperties.get("TABLE_NAME");

	public static void addHBaseRecordDirectBatch(List<HBaseRecord> batch) throws Exception {
		HbaseOperations.addRecordBatchDirect(TABLE_NAME,batch);
	}
	
	public static void fetchHBaseRecordDirectBatchScan(List<HBaseRecord> batch) throws Exception {
		HbaseOperations.getRecordBatchDirect(TABLE_NAME,batch);
	}

}
