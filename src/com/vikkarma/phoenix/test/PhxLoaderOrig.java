package com.vikkarma.phoenix.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.util.Pair;

public class PhxLoaderOrig {

  public void testConcurrentUpserts() throws Exception {
    String createTableDDL =
        "CREATE TABLE IF NOT EXISTS T (\n" + " ROW_KEY_ID VARCHAR NOT NULL,\n"
            + " ROW_VAL VARCHAR,\n" + " CONSTRAINT PK PRIMARY KEY (ROW_KEY_ID)\n"
            + ") DEFERRED_LOG_FLUSH=true,SALT_BUCKETS=16";
    Connection conn = PhoenixConnectionUtil.getPhxConnection();
    conn.createStatement().execute(createTableDDL);
    conn.close();
    int numThreads = 16;
    int numRecordsToUpsert = 6400000;
    int batchSize = 6400;
    int numBatches = numRecordsToUpsert / batchSize;
    List<Pair<String, String>> testData = generateTestData(numRecordsToUpsert);
    System.out.println("Generated Data ....");
    ExecutorService writeService = Executors.newFixedThreadPool(numThreads);
    List<Future<Void>> writeFutures = new ArrayList<Future<Void>>();
    for (int i = 0; i < numBatches; i++) {
      int batchStartIndex = i * batchSize;
      int batchEndIndex = (i + 1) * batchSize - 1;
      writeFutures.add(writeService.submit(new InsertBatchRunnable(testData.subList(
        batchStartIndex, batchEndIndex + 1))));
    }
    System.out.println("Waiting for threads to complete ....");
    for (Future future : writeFutures) {
      future.get();
    }
    writeService.shutdown();
    System.out.println("Done futures complete ....");
  }

  private static class InsertBatchRunnable implements Callable<Void> {
    private final List<Pair<String, String>> batch;
    private static final String upsertDml = "UPSERT INTO T (ROW_KEY_ID, ROW_VAL) VALUES (?, ?)";

    InsertBatchRunnable(List<Pair<String, String>> batch) {
      this.batch = batch;
    }

    @Override
    public Void call() throws Exception {

      Connection conn = PhoenixConnectionUtil.getPhxConnection();
      for (Pair<String, String> record : batch) {
        PreparedStatement stmt = conn.prepareStatement(upsertDml);
        stmt.setString(1, record.getFirst());
        stmt.setString(2, record.getSecond());
        stmt.execute();
        stmt.close();
      }      
      conn.commit();
      conn.close();
      return null;
    }
  }

  private List<Pair<String, String>> generateTestData(int numRecords) {
    String rowKey;
    String value;
    List<Pair<String, String>> records = new ArrayList<Pair<String, String>>();
    for (int i = 1; i <= numRecords; i++) {
      rowKey = "key" + i;
      value = "value" + i;
      records.add(new Pair<String, String>(rowKey, value));
    }
    return records;
  }
  
  public static void main(String[] args) throws Exception {
    PhxLoaderOrig pxorig = new PhxLoaderOrig();
    pxorig.testConcurrentUpserts();
  }

}
