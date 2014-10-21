package com.vikkarma.hbase.enums;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum HBaseConnectionType
{
	PRIMARY("PRIMARY"),DR("DR");
	private static final Map<String, HBaseConnectionType> lookup = new HashMap<String, HBaseConnectionType>();

	static
	{
		for (HBaseConnectionType s : EnumSet.allOf(HBaseConnectionType.class))
			lookup.put(s.getCode(), s);
	}

	private String code;

	private HBaseConnectionType(String code)
	{

		this.code = code;
	}

	public String getCode()
	{
		return code;
	}

	public static HBaseConnectionType get(String code)
	{
		return lookup.get(code);
	}
}
