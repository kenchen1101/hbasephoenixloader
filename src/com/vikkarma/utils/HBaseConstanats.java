package com.vikkarma.utils;

public class HBaseConstanats {
	private static final HBaseTestProperties testProperties = HBaseTestProperties
			.getInstance();
	//row key 
	public static final String exchangePrefix = testProperties
			.get("EXCHG_PREFIX");
	public static final String topicPrefix = testProperties
			.get("TOPIC_PREFIX");
	//htable definition 
	public static final String[] historyColumnFamily={"HCF"}; //HistoryKey=E:T:Timestamp(millis)
	public static final String historyColumnQualifier="HQ"; 

}
