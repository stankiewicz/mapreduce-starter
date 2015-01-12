package pl.stankiewicz.bigdata.starter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceDraftWithCustomWriteable extends Configured implements
		Tool {

	public static class CustomKeyWritable implements
			WritableComparable<CustomKeyWritable> {

		String k1;
		String k2;

		public void readFields(DataInput arg0) throws IOException {
			k1 = arg0.readUTF();
			k2 = arg0.readUTF();
		}

		public void write(DataOutput arg0) throws IOException {
			arg0.writeUTF(k1);
			arg0.writeUTF(k2);
		}

		public int compareTo(CustomKeyWritable arg0) {

			int res = k1.compareTo(arg0.k1);
			if (res == 0) {
				res = k2.compareTo(arg0.k2);
			}
			return res;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((k2 == null) ? 0 : k2.hashCode());
			result = prime * result + ((k1 == null) ? 0 : k1.hashCode());
			return result;
		}

	}

	public static class CustomValueWritable implements Writable {

		long v1;
		int v2;

		public void readFields(DataInput arg0) throws IOException {
			v1 = arg0.readLong();
			v2 = arg0.readInt();
		}

		public void write(DataOutput arg0) throws IOException {
			arg0.writeLong(v1);
			arg0.writeInt(v2);
		}

	}

	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf(), "fill me");

		// TODO fill it

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MapReduceDraftWithCustomWriteable(), args);
		System.exit(res);
	}
}
