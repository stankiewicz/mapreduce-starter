package pl.stankiewicz.bigdata.starter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MapReduceDraftTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setup() {
		//TODO set mr drivers, check key, value types
		MapReduceDraft.MyMapper myMapper = new MapReduceDraft.MyMapper();
		mapDriver = MapDriver.newMapDriver(myMapper);
		MapReduceDraft.MyReducer myReducer = new MapReduceDraft.MyReducer();
		reduceDriver = ReduceDriver.newReduceDriver(myReducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(myMapper,
				myReducer);
	}

	@Test
	public void testMapper() throws IOException {
		// TODO fill input and output
		//mapDriver.withInput;
		//mapDriver.withOutput;
		mapDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {

		// TODO fill input and output, maybe list of inputs and test against list of outputs?
		mapReduceDriver.runTest();
	}


	@Test
	public void testReducer() throws IOException {
		// TODO fill input and output
		reduceDriver.runTest();
	}

}
