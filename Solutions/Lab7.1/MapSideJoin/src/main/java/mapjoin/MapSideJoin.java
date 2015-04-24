package mapjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoin extends Configured implements Tool {

	public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Stock, StockPrices> {
		private String stockSymbol;
		private HashMap<Stock,Double> stocks = new HashMap<Stock,Double>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			stockSymbol = context.getConfiguration().get("stockSymbol");
				BufferedReader in = new BufferedReader(new FileReader("NYSE_dividends_A.csv"));
				String currentLine = "";
				while((currentLine = in.readLine()) != null) {
					String [] words = StringUtils.split(currentLine,'\\',',');
					if(words[1].equals(stockSymbol)) {
						Stock stock = new Stock();
						stock.setSymbol(words[1]);
						stock.setDate(words[2]);
						double dividend = Double.parseDouble(words[3]);
						stocks.put(stock, dividend);
					}
				}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] words = StringUtils.split(value.toString());
			if(stockSymbol.equals(words[1])) {
				Stock stock = new Stock();
				stock.setSymbol(words[1]);
				stock.setDate(words[2]);
				if(stocks.containsKey(stock)) {
					double dividend = stocks.get(stock);
					StockPrices prices = new StockPrices();
					prices.setDividend(dividend);
					prices.setClosingPrice(Double.parseDouble(words[6]));
					context.write(stock, prices);
				}
			}
		}
	}


	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MapSideJoinJob");
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		

		Path out = new Path("joinoutput");
		out.getFileSystem(conf).delete(out,true);
		FileInputFormat.setInputPaths(job, new Path("stocks"));
		FileOutputFormat.setOutputPath(job, out);
		
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Stock.class);
		job.setMapOutputValueClass(StockPrices.class);
		
		job.setNumReduceTasks(0);
		job.addCacheFile(new URI("dividends/NYSE_dividends_A.csv"));
		conf.set("stockSymbol", args[0]);
		conf.set(TextOutputFormat.SEPERATOR, ",");

		return job.waitForCompletion(true)?0:1;

	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new MapSideJoin(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
