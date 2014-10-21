
package com.vikkarma.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.vikkarma.hbase.enums.MyLogLevel;


public class CommonTestProperties {
	private static CommonTestProperties commonTestProperties;
	private static Properties properties;
	private static String commonTestPropertiesFilePath = "config/CommonTest.properties";
	

	protected CommonTestProperties(){
		System.out.println("Loading Properties File " + commonTestPropertiesFilePath);
		File file = new File(commonTestPropertiesFilePath);
		properties = new Properties();
        FileInputStream fis;
        try {
            fis = new FileInputStream(file);
            properties.load(fis);
        } catch (FileNotFoundException e) {
        	MyLogger.mylogger.severe("Exception while reading test properties" + commonTestPropertiesFilePath);
        } catch (IOException e) {
        	MyLogger.mylogger.severe("Exception while reading test properties" + commonTestPropertiesFilePath);
        }
	}
	
	public synchronized static CommonTestProperties getInstance(){
		if (commonTestProperties==null){
			commonTestProperties = new CommonTestProperties();
		}
		return commonTestProperties;
	}
	
	public static MyLogLevel getMyLogLevel(String key)
	{
		String code = properties.getProperty(key);
		return MyLogLevel.get(code);
	}
	
	public String get(String key){
		return properties.getProperty(key);
	}
	
	public String get(String key, String defval){
		return properties.getProperty(key, defval);
	}

	public Integer getInteger(String key){
		return new Integer(properties.getProperty(key));
	}
	
	public Integer getInteger(String key, int defval){
		return new Integer(properties.getProperty(key, ""+defval));
	}

	public Long getLong(String key){
		return new Long(properties.getProperty(key));
	}
	
	public Long getLong(String key, long defval){
		return new Long(properties.getProperty(key, ""+defval));
	}

	public Boolean getBoolean(String key){
		return new Boolean(properties.getProperty(key));
	}
	
	public Boolean getBoolean(String key, boolean defval){
		return new Boolean(properties.getProperty(key, ""+defval));
	}

	public Object clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}
}
