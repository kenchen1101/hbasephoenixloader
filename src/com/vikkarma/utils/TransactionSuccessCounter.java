package com.vikkarma.utils;

import java.util.concurrent.atomic.AtomicInteger;

/*
 * Counter for concurrent counting
 */
public class TransactionSuccessCounter {
	private AtomicInteger transactionCtr = new AtomicInteger();
	
	public TransactionSuccessCounter(int initVal) {
		this.transactionCtr.set(initVal);
	}
	
    public void setCount(int newVal) {
        this.transactionCtr.set(newVal);
    }
    
    public int getCount() {
        return this.transactionCtr.get();
    }
    
    public int incrementCount() {
        return transactionCtr.incrementAndGet();
    }    
	
    public int decrementCount() {
        return transactionCtr.decrementAndGet();
    }    

    public int offsetCount(int offset) {
        return transactionCtr.getAndAdd(offset);
    }   
    
    

}
