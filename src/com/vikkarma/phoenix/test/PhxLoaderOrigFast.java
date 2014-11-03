package com.vikkarma.phoenix.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PhxLoaderOrigFast {
  
  private static final String val = "0123450123456789A0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzpqrstuvwxyz";
  //private static final String val = "012345";

  public void testConcurrentUpserts() throws Exception {
    String createTableDDL =
        "CREATE TABLE IF NOT EXISTS PT1_1 (\n" + " ROW_KEY_ID VARCHAR NOT NULL,\n"
            + " ROW_VAL VARCHAR,\n" + " CONSTRAINT PK PRIMARY KEY (ROW_KEY_ID)\n"
            + ") DEFERRED_LOG_FLUSH=true,SALT_BUCKETS=16";
    Connection conn = PhoenixConnectionUtil.getPhxConnection();
    conn.createStatement().execute(createTableDDL);
    conn.close();
    int numThreads = 16;
    int numRecordsToUpsert = 6400000;
    int batchSize = 6400;
    int numBatches = numRecordsToUpsert / batchSize;
    //generate data batches
    List<String> testData = generateTestData(numRecordsToUpsert);
    List<List<String>> testDataSubLists = new ArrayList<List<String>>(); 
    System.out.println("Generated Data ....");
    ExecutorService writeService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numBatches; i++) {
      int batchStartIndex = i * batchSize;
      int batchEndIndex = (i + 1) * batchSize - 1;
      testDataSubLists.add(testData.subList(batchStartIndex, batchEndIndex + 1));
    }
    
    //Insert data batches
    List<Future<Void>> writeFutures = new ArrayList<Future<Void>>();
    for (int i=0; i<numBatches; i++) {
      writeFutures.add(writeService.submit(new InsertBatchRunnable(testDataSubLists.get(i))));
    }
    System.out.println("Waiting for threads to complete ....");
    for (Future future : writeFutures) {
      future.get();
    }
    writeService.shutdown();
    System.out.println("Done futures complete ....");
  }

  private static class InsertBatchRunnable implements Callable<Void> {
    private final List<String> batch;
    private static final String upsertDml = "UPSERT INTO PT1_1 (ROW_KEY_ID, ROW_VAL) VALUES (?, ?)";

    InsertBatchRunnable(List<String> batch) {
      this.batch = batch;
    }

    @Override
    public Void call() throws Exception {

      Connection conn = PhoenixConnectionUtil.getPhxConnection();
      for (String record : batch) {
        PreparedStatement stmt = conn.prepareStatement(upsertDml);
        stmt.setString(1, record);
        stmt.setString(2, val);
        stmt.execute();
        stmt.close();
      }
      conn.commit();
      conn.close();
      return null;
    }
  }

  private List<String> generateTestData(int numRecords) {
    String rowKey;
    String value;
    List<String> records = new ArrayList<String>();
    for (int i = 1; i <= numRecords; i++) {
      rowKey = "key" + i;
      records.add(rowKey);
    }
    return records;
  }
  
  public static void main(String[] args) throws Exception {
    PhxLoaderOrigFast pxorig = new PhxLoaderOrigFast();
    pxorig.testConcurrentUpserts();
  }

}
