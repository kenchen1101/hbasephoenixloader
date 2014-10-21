package com.vikkarma.utils;

import java.util.Arrays;

public class ShutDownHook extends Thread{
	private static int waitTime;
		
	public static void setWaitTime(int waitTime) {
		ShutDownHook.waitTime = waitTime;
	}

	public void run(){
		MyLogger.mylogger.info("Initiated Shutdown hook, exiting in " + waitTime + " seconds ...");
		try {
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {	
			MyLogger.mylogger.severe("ShutDownHook " + e.getMessage() + Arrays.toString(e.getStackTrace()));
		}

	}
}
