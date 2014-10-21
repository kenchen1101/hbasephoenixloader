package com.vikkarma.hbase.enums;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum HBaseTableType
{
	BULK("BULK"),UNSPLIT_TABLE("UNSPLIT_TABLE"),SPLIT_TABLE("SPLIT_TABLE");
	private static final Map<String, HBaseTableType> lookup = new HashMap<String, HBaseTableType>();

	static
	{
		for (HBaseTableType s : EnumSet.allOf(HBaseTableType.class))
			lookup.put(s.getCode(), s);
	}

	private String code;

	private HBaseTableType(String code)
	{

		this.code = code;
	}

	public String getCode()
	{
		return code;
	}

	public static HBaseTableType get(String code)
	{
		return lookup.get(code);
	}
}
