package com.vikkarma.hbase.data;

public class HBaseRecord {
	private String key;
	private String columnqual;
	private String val;
	
	public HBaseRecord(String key, String columnqual, String val) {
		this.key = key;
		this.columnqual = columnqual;
		this.val=val;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		this.val = val;
	}
	
	public String getColumnqual() {
		return columnqual;
	}

	public void setColumnqual(String columnqual) {
		this.columnqual = columnqual;
	}
	
	

}
