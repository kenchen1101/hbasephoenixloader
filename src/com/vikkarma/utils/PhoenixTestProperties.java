
package com.vikkarma.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public class PhoenixTestProperties {
	private static PhoenixTestProperties phoenixTestProperties;
	private static Properties properties;
	private static String phoenixTestPropertiesFilePath = "config/PhoenixTest.properties";
	

	private PhoenixTestProperties(){
		File file = new File(phoenixTestPropertiesFilePath);
		properties = new Properties();
        FileInputStream fis;
        try {
            fis = new FileInputStream(file);
            properties.load(fis);
        } catch (FileNotFoundException e) {
        	MyLogger.mylogger.severe("Exception while reading test properties" + phoenixTestPropertiesFilePath);
        } catch (IOException e) {
        	MyLogger.mylogger.severe("Exception while reading test properties" + phoenixTestPropertiesFilePath);
        }
	}
	
	public synchronized static PhoenixTestProperties getInstance(){
		if (phoenixTestProperties==null){
			phoenixTestProperties = new PhoenixTestProperties();
		}
		return phoenixTestProperties;
	}
	

	
	public static void setTestSetupPropertiesFilePath(
			String phoenixTestPropertiesFilePath1) {
		phoenixTestPropertiesFilePath = phoenixTestPropertiesFilePath1;
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

    public void setProperty(String key, String value) {
       properties.setProperty(key, value);
    }

}
