package com.vikkarma.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class RandomKeyUtils {
	
  private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private static Random rnd = new Random();
	private static String fixedStr = null;

	
	public static String getRandomString( int len ) 
	{
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) 
	      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
	   return sb.toString();
	}
	
	public static String getFixedString( int len ) 
	{
		if (fixedStr != null ) {
			return fixedStr;
		}
		else {
			fixedStr = getRandomString(len);
		}
		return fixedStr;
	}
	
	public static String getDate() {
		Date myDate = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss");
		String myDateString = sdf.format(myDate);
		return myDateString;
	}
	
	public static int getRandomIntInrange(int Min, int Max) {
		return Min + (int)(Math.random() * ((Max - Min) + 1));		
	}
}
