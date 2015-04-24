package hbase;

public class StockConstants {
	public static final byte [] PRICE_COLUMN_FAMILY = "p".getBytes();
	public static final byte [] HIGH_QUALIFIER = "high".getBytes();
	public static final byte [] LOW_QUALIFIER = "low".getBytes();
	public static final byte [] CLOSING_QUALIFIER = "close".getBytes();
	public static final byte [] VOLUME_QUALIFIER = "vol".getBytes();
}
