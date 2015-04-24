package average;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import average.AverageJob.AverageMapper;
import average.AverageJob.AverageReducer;

public class AverageJobTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	
	@Before
	public void setup() {
		AverageMapper mapper = new AverageMapper();
		AverageReducer reducer = new AverageReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		LongWritable inputKey = new LongWritable(0);
		Text inputValue = new Text("Abbeville, SC,45001,6581,7471,6787,195278,302280,29673,40460,3042,3294");
		mapDriver.withInput(inputKey, inputValue);
		Text outputKey = new Text("SC");
		Text outputValue = new Text("40460,1");
		mapDriver.withOutput(outputKey, outputValue);
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		Text inputKey = new Text("SC");
		List<Text> inputValues = new ArrayList<Text>();
		inputValues.add(new Text("122500,3"));
		inputValues.add(new Text("38100,1"));
		reduceDriver.setInput(inputKey, inputValues);
		
		DoubleWritable outputValue = new DoubleWritable(40150.0);
		reduceDriver.withOutput(inputKey, outputValue);
		
		reduceDriver.runTest();
	}
}
