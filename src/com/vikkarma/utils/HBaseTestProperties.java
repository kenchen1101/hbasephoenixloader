
package com.vikkarma.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.vikkarma.hbase.enums.HBaseConnectionType;
import com.vikkarma.hbase.enums.HBaseFilterTypes;


public class HBaseTestProperties {
	private static HBaseTestProperties hbaseTestProperties;
	private static Properties properties;
	private static String hbaseTestPropertiesFilePath = "config/HBaseTest.properties";
	

	private HBaseTestProperties(){
		File file = new File(hbaseTestPropertiesFilePath);
		properties = new Properties();
        FileInputStream fis;
        try {
            fis = new FileInputStream(file);
            properties.load(fis);
        } catch (FileNotFoundException e) {
        	MyLogger.mylogger.severe("Exception while reading test properties" + hbaseTestPropertiesFilePath);
        } catch (IOException e) {
        	MyLogger.mylogger.severe("Exception while reading test properties" + hbaseTestPropertiesFilePath);
        }
	}
	
	public synchronized static HBaseTestProperties getInstance(){
		if (hbaseTestProperties==null){
			hbaseTestProperties = new HBaseTestProperties();
		}
		return hbaseTestProperties;
	}
	
	public long[] getLongArray(String key){
		String keystr = properties.getProperty(key);
		String[] keyStrArray =  keystr.split("#");
		long longarray[] = new long[keyStrArray.length];
		for (int i = 0; i < keyStrArray.length; i++) {
			longarray[i] = Long.parseLong(keyStrArray[i]);
		}
		return longarray;
	}
	public static HBaseFilterTypes getHBaseFilterTypes(String key)
	{
		String code = properties.getProperty(key);
		return HBaseFilterTypes.get(code);
	}
	
	public static HBaseConnectionType getHBaseConnectionType(String key)
	{
		String code = properties.getProperty(key);
		return HBaseConnectionType.get(code);
	}
	
	public static void setTestSetupPropertiesFilePath(
			String gdbTestPropertiesFilePath1) {
		hbaseTestPropertiesFilePath = gdbTestPropertiesFilePath1;
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
