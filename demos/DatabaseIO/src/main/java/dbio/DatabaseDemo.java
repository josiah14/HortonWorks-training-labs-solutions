package dbio;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DatabaseDemo extends Configured implements Tool {

	public static class DatabaseDemoMapper extends
			Mapper<LongWritable, Employee, Text, DoubleWritable> {
		
		private Text outputKey = new Text();
		private DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		protected void map(LongWritable key, Employee value, Context context)
				throws IOException, InterruptedException {
			outputKey.set(value.getDept());
			outputValue.set(value.getSalary());
			context.write(outputKey, outputValue);
		}
	}	

	public static class DatabaseDemoReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		private DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			while(values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf(), "DatabaseDemoJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(DatabaseDemo.class);

		Path out = new Path("dboutput");
		FileOutputFormat.setOutputPath(job, out);
		out.getFileSystem(conf).delete(out, true);
		
		job.setMapperClass(DatabaseDemoMapper.class);
		job.setReducerClass(DatabaseDemoReducer.class);
		job.setInputFormatClass(DBInputFormat.class);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mydb", "root", "");
		String [] fields = {"firstName","lastName","department","salary"};
		DBInputFormat.setInput(job, Employee.class, "employees", null, "id", fields);
		
		job.setOutputFormatClass(TextOutputFormat.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true)?0:1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new DatabaseDemo(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
