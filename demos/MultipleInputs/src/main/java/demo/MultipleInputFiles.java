package demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleInputFiles extends Configured implements Tool {

	public static class NamesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputValue = new Text();
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String currentLine = value.toString();
			String [] words = StringUtils.split(currentLine, '\\', '\t');
			outputKey.set(words[0]);
			outputValue.set(words[1]);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class StatesMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outputValue = new Text();
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String currentLine = value.toString();
			String [] words = StringUtils.split(currentLine, '\\', ',');
			outputKey.set(words[0]);
			outputValue.set(words[1]);
			context.write(outputKey, outputValue);
		}
	}

	public static class MultiInputReducer extends Reducer<Text, Text, Text, Text> {
		private Text outputValue = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			StringBuilder output = new StringBuilder();
			for(Text value : values) {
				output.append(value.toString() + ",");
			}
			outputValue.set(output.toString());
			context.write(key, outputValue);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MultipleInputFilesJob");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		Path names = new Path("multiinputs/names_ids.txt");
		Path states = new Path("multiinputs/ids_states.txt");
		MultipleInputs.addInputPath(job, names, TextInputFormat.class, NamesMapper.class);
		MultipleInputs.addInputPath(job, states, TextInputFormat.class, StatesMapper.class);
		
		Path out = new Path("multiinputs/output");
		out.getFileSystem(conf).delete(out, true);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setReducerClass(MultiInputReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), 
							new MultipleInputFiles(),
							args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);
	}

}
