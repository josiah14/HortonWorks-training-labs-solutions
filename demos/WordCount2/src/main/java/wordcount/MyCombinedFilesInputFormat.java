package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyCombinedFilesInputFormat extends CombineFileInputFormat<LongWritable, Text> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader(
					(CombineFileSplit) split,
					context,
					MyCombinedFilesRecordReader.class
					);
	}

	public static class MyCombinedFilesRecordReader extends RecordReader<LongWritable, Text> {
		private int index;
		private LineRecordReader reader;
		
		public MyCombinedFilesRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) {
			this.index = index;
			reader = new LineRecordReader();
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			CombineFileSplit cfsplit = (CombineFileSplit) split;
			FileSplit fileSplit = new FileSplit(cfsplit.getPath(index),
												cfsplit.getOffset(index),
												cfsplit.getLength(index),
												cfsplit.getLocations()
					);
			reader.initialize(fileSplit, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reader.nextKeyValue();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return reader.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
		
	}
}
