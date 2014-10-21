package com.vikkarma.utils;

import java.util.Properties;

public class GetPropertyValues {

	public String get(Properties properties, String key){
		return properties.getProperty(key);
	}
	
	public String get(Properties properties, String key, String defval){
		return properties.getProperty(key, defval);
	}

	public Integer getInteger(Properties properties, String key){
		return new Integer(properties.getProperty(key));
	}
	
	public Integer getInteger(Properties properties, String key, int defval){
		return new Integer(properties.getProperty(key, ""+defval));
	}

	public Long getLong(Properties properties, String key){
		return new Long(properties.getProperty(key));
	}
	
	public Long getLong(Properties properties, String key, long defval){
		return new Long(properties.getProperty(key, ""+defval));
	}

	public Boolean getBoolean(Properties properties, String key){
		return new Boolean(properties.getProperty(key));
	}
	
	public Boolean getBoolean(Properties properties, String key, boolean defval){
		return new Boolean(properties.getProperty(key, ""+defval));
	}

	public Object clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}
}
