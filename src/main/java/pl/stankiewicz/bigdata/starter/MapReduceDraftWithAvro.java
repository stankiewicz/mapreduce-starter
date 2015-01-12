package pl.stankiewicz.bigdata.starter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceDraftWithAvro extends Configured implements Tool {

	//public static class MyMapper
	//		extends
	//		Mapper<LongWritable, Text, AvroKey<ExampleKey>, AvroValue<ExampleValue>> {
	//}

	//public static class MyReducer
	//		extends
	//		Reducer<AvroKey<ExampleKey>, AvroValue<ExampleValue>, Text, NullWritable> {
	//}

	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "word count");

		// AvroJob.setMapOutputKeySchema(job, ExampleKey.getClassSchema());
		// AvroJob.setMapOutputValueSchema(job, ExampleValue.getClassSchema());

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MapReduceDraftWithAvro(), args);
		System.exit(res);
	}
}
