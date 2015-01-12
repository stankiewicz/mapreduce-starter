package pl.stankiewicz.bigdata.starter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceDraft extends Configured implements Tool {

	// TODO  configure Mapper generic types <LongWritable (k1), valueIn (v1),
	// keyOut (k2), ValueOut (v2)>
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);
		Text text = new Text();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			// TODO parse/filter value, set key to text, set value if
			// different than '1'
			context.write(text, one);

		}
	}

	// TODO configure Reducer generic types <k2, v2, k3, v3>
	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private static IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text text, Iterable<IntWritable> list,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {

			// TODO agregate, summarize, write result
			arg2.write(text, result);
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "job name");

		job.setJarByClass(MapReduceDraft.class);
		
		// TODO set mapper
		// job.setMapperClass
		
		// TODO is combiner required?
		// job.setCombinerClass
		
		// TODO set reducer
		//job.setReducerClass

		// is output of mapper different than output of reducer?
		//job.setMapOutputValueClass
		
		// TODO configure output k3, v3
		//job.setOutputKeyClass
		//job.setOutputValueClass

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapReduceDraft(),
				args);
		System.exit(res);
	}
}
