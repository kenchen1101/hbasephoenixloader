package com.vikkarma.hbase.data;

public class ETKey {
	private int xchgIndex;
	private int topicIndex;
	
	public ETKey(int xchgIndex, int topicIndex) {
		this.xchgIndex = xchgIndex;
		this.topicIndex = topicIndex;
	}
	
	public int getXchgIndex() {
		return xchgIndex;
	}
	public void setXchgIndex(int xchgIndex) {
		this.xchgIndex = xchgIndex;
	}
	public int getTopicIndex() {
		return topicIndex;
	}
	public void setTopicIndex(int topicIndex) {
		this.topicIndex = topicIndex;
	}
	
	
	

}
