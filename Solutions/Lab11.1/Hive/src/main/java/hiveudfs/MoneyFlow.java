package hiveudfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class MoneyFlow extends UDF {
  private DoubleWritable result = new DoubleWritable();

  public DoubleWritable evaluate(double high, double low, double close, int volume) {
    double typicalPrice = (high + low + close) / 3;
    double moneyFlow = typicalPrice * volume;
    result.set(moneyFlow);
    return result;
  }
}
