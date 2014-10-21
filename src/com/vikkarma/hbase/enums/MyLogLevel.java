package com.vikkarma.hbase.enums;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum MyLogLevel
{
	
	FINE("FINE"),FINER("FINER"),FINEST("FINEST"),INFO("INFO"),WARNING("WARNING"),SEVERE("SEVERE"),OFF("OFF") ;
	private static final Map<String, MyLogLevel> lookup = new HashMap<String, MyLogLevel>();

	static
	{
		for (MyLogLevel s : EnumSet.allOf(MyLogLevel.class))
			lookup.put(s.getCode(), s);
	}

	private String code;

	private MyLogLevel(String code)
	{

		this.code = code;
	}

	public String getCode()
	{
		return code;
	}

	public static MyLogLevel get(String code)
	{
		return lookup.get(code);
	}
}
