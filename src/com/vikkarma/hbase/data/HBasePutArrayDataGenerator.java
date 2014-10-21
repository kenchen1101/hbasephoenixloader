package com.vikkarma.hbase.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.vikkarma.hbase.loader.HBaseLoadParams;
import com.vikkarma.utils.HBaseConstanats;
import com.vikkarma.utils.MyLogger;

public class HBasePutArrayDataGenerator {
	//Insert test properties
	private static final int INSERT_BULK = HBaseLoadParams.getWriteBatchSize();
	private static final int BULK_SIZE = HBaseLoadParams.getBulkSize();
	private LoadHBaseDataFile histData = null;
	private static List <List<Row>> batchArray = null; 
	
	public HBasePutArrayDataGenerator() {
	   histData = new LoadHBaseDataFile();
	   batchArray = new ArrayList<List<Row>>();
	}

	public List<List<Row>> getBatchArray() {
		generatePutDataArray();
		cleanUp();
		return batchArray;
	}

	/**
	 * read the hbase data and load them in mem
	 * in the batchArray before starting the put storm
	 * will work for only small amount of data <5G to 
	 * create spike put loads 
	 */
	private void generatePutDataArray() {
	  MyLogger.mylogger.info("loading hbase put data in memory ...");
	  long startBatchGenerationTime = System.currentTimeMillis();
		long numNodeOps = 10;
		while (numNodeOps != 0) {
			numNodeOps = histData.readBulkHistoryDataFile(BULK_SIZE);
			int histindex = 0;			 
			for (int i = 0; i < histData.getHbaseDataChunk().size(); i = i + INSERT_BULK) {
				List<Row> batch = new ArrayList<Row>();
				for (int j = 0; j < INSERT_BULK; j++) {
					if (histindex < histData.getHbaseDataChunk().size()) {
						Put put = new Put(Bytes.toBytes(histData.getHbaseDataChunk().get(histindex).getKey()));
						put.add(Bytes.toBytes(HBaseConstanats.historyColumnFamily[0]), Bytes.toBytes(histData.getHbaseDataChunk().get(histindex).getColumnqual()),
								Bytes.toBytes(histData.getHbaseDataChunk().get(histindex).getVal()));
						batch.add(put);
					}
					histindex++;
				}
				batchArray.add(batch);
			}			
		}
    // log time required
    long endBatchGenerationTime = System.currentTimeMillis() - startBatchGenerationTime;
    MyLogger.mylogger
        .info("Completed HBasePutArrayDataGenerator generating in-memory put batches starting from  "
            + startBatchGenerationTime + ". Took total time  " + endBatchGenerationTime + " ms");
	}
	
	public void cleanUp() {
		MyLogger.mylogger
				.info("HBasePutArrayDataGenerator :: Closing data file  ....");
		histData.closecsv();
	}

}
