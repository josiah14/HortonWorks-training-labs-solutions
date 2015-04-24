package hbasemr;

import static hbasemr.StockConstants.CLOSING_QUALIFIER;
import static hbasemr.StockConstants.DATE_QUALIFIER;
import static hbasemr.StockConstants.INFO_COLUMN_FAMILY;
import static hbasemr.StockConstants.PRICE_COLUMN_FAMILY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxClosingPriceJob extends Configured implements Tool {

	public static class MaxClosingPriceMapper extends TableMapper<Text, Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			Cell closingPrice = value.getColumnLatestCell(PRICE_COLUMN_FAMILY, CLOSING_QUALIFIER);
			String keyString = Bytes.toString(key.get());
			String date = keyString.substring(0, 10);
			String symbol = keyString.substring(10, keyString.length());
			outputKey.set(symbol);
			outputValue.set(date + Bytes.toDouble(CellUtil.cloneValue(closingPrice)));
			context.write(outputKey, outputValue);
		}
		
	}
	
	public static class MaxClosingPriceReducer extends TableReducer<Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values,	Context context)
				throws IOException, InterruptedException {
			double max = 0.0;
			String maxDate = "";
			for(Text value : values) {
				String current = value.toString();
				double currentPrice = Double.parseDouble(current.substring(8, current.length()));
				if(currentPrice > max) {
					max = currentPrice;
					maxDate = current.substring(0,10);
				}
			}
			Put put = new Put(key.getBytes());
			put.add(INFO_COLUMN_FAMILY, CLOSING_QUALIFIER, Bytes.toBytes(max));
			put.add(INFO_COLUMN_FAMILY, DATE_QUALIFIER, Bytes.toBytes(maxDate));
			context.write(key, put);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(getConf());
		Job job = Job.getInstance(conf, "MaxClosingPriceJob");
		job.setJarByClass(MaxClosingPriceJob.class);
		TableMapReduceUtil.addDependencyJars(job);

		Scan scan = new Scan();
		scan.addColumn(PRICE_COLUMN_FAMILY, CLOSING_QUALIFIER);
		TableMapReduceUtil.initTableMapperJob("stocks", scan, MaxClosingPriceMapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob("stockhighs", MaxClosingPriceReducer.class, job);
		
		return job.waitForCompletion(true)?0:1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new MaxClosingPriceJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}
}
