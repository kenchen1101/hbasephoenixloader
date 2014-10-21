package com.vikkarma.hbase.enums;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum HBaseFilterTypes {
	// ColumnCountGetFilter, ColumnPaginationFilter, ColumnPrefixFilter,
	// ColumnRangeFilter,
	// CompareFilter, FirstKeyOnlyFilter, FuzzyRowFilter, InclusiveStopFilter,
	// KeyOnlyFilter,
	// MultipleColumnPrefixFilter, PageFilter, PrefixFilter, RandomRowFilter,
	// SingleColumnValueFilter,
	// SkipFilter, TimestampsFilter, WhileMatchFilter
	RowKeyFilter("RowKeyFilter"), SingleRowKey("SingleRowKey"), BulkRowKey("BulkRowKey"), PrefixFilter("PrefixFilter"), ColumnRangeFilter(
			"ColumnRangeFilter"), QualifierFilter("QualifierFilter");

	private static final Map<String, HBaseFilterTypes> lookup = new HashMap<String, HBaseFilterTypes>();

	static {
		for (HBaseFilterTypes s : EnumSet.allOf(HBaseFilterTypes.class))
			lookup.put(s.getCode(), s);
	}

	private String code;

	private HBaseFilterTypes(String code) {
		this.code = code;
	}

	public String getCode() {
		return code;
	}

	public static HBaseFilterTypes get(String code) {
		return lookup.get(code);
	}

	public static HBaseFilterTypes getGraphType(HBaseFilterTypes dbtype) {
		switch (dbtype) {
		case BulkRowKey:
			return HBaseFilterTypes.BulkRowKey;
		case SingleRowKey:
			return HBaseFilterTypes.SingleRowKey;
		case PrefixFilter:
			return HBaseFilterTypes.PrefixFilter;
		case ColumnRangeFilter:
			return HBaseFilterTypes.ColumnRangeFilter;
		case QualifierFilter:
			return HBaseFilterTypes.QualifierFilter;
		default:
			return null;

		}
	}

}