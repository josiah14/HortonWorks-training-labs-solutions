package dbio;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DBOutputDemo extends Configured implements Tool {

	public static class DBOutputDemoMapper extends
			Mapper<LongWritable, Employee, LongWritable, Employee> {
		
		@Override
		protected void map(LongWritable key, Employee value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}	

	public static class DBOutputDemoReducer extends
			Reducer<LongWritable, Employee, SalaryData, NullWritable> {
		private SalaryData outputKey = new SalaryData();
		private NullWritable outputValue = NullWritable.get();
		
		@Override
		protected void reduce(LongWritable key, Iterable<Employee> values, Context context)
				throws IOException, InterruptedException {
			Employee current = values.iterator().next();
			outputKey.setDepartment(current.getDept());
			outputKey.setSalary(current.getSalary());
			outputKey.setMapkey((int) key.get());
			context.write(outputKey, outputValue);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf(), "DBOutputDemoJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(DBOutputDemo.class);
		
		job.setMapperClass(DBOutputDemoMapper.class);
		job.setReducerClass(DBOutputDemoReducer.class);

		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/mydb", "root", "");
		String [] inputFields = {"firstName","lastName","department","salary"};
		DBInputFormat.setInput(job, Employee.class, "employees", null, "id", inputFields);
		
		String [] outputFields = {"department", "salary", "mapkey"};
		DBOutputFormat.setOutput(job, "salarydata", outputFields);
		
		job.setOutputKeyClass(SalaryData.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Employee.class);

		return job.waitForCompletion(true)?0:1;
	}


	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(),  new DBOutputDemo(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);

	}

}
