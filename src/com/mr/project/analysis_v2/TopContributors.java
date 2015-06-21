package com.mr.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopContributors {
	public static int k;

	public static class HbaseMapper extends TableMapper<Text, Text> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] COUNT = "count".getBytes();
		public static final byte[] TAG = "tag".getBytes();
		public static final byte[] USERNAME = "username".getBytes();
		Map<String, List<Record>> recordMap;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			recordMap = new HashMap<String, List<Record>>();
		}

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws InterruptedException, IOException {

			String userName = new String(value.getValue(DATA, USERNAME));
			Long count = Bytes.toLong(value.getValue(DATA, COUNT));			
			String tag = new String(value.getValue(DATA, TAG));
			List<Record> data = recordMap.containsKey(tag) ? recordMap.get(tag)
					: new ArrayList<Record>();
			data.add(new Record(count, userName));
			recordMap.put(tag, data);

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Entry<String, List<Record>> entry : recordMap.entrySet()) {
				String tag = entry.getKey();
				List<Record> list = entry.getValue();
				Collections.sort(list);
				Collections.reverse(list);
				int limit = Math.min(k, list.size());
				for (int i = 0; i < limit; i++) {
					Record record = list.get(i);

					context.write(new Text(tag), new Text(record.getCount()
							+ "," + record.getUserName()));
				}
			}
		}

	}

	public static class TopContributorReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String tag = new String(key.toString());
			List<Record> list = new ArrayList<Record>();
			for (Text val : values) {
				String[] record = val.toString().split(",");
				list.add(new Record(Long.parseLong(record[0]), record[1]));
			}

			Collections.sort(list);
			Collections.reverse(list);
			int limit = Math.min(k, list.size());
			StringBuffer result = new StringBuffer();
			for (int i = 0; i < limit; i++) {
				Record record = list.get(i);
				result.append(record.getUserName() + "(" + record.getCount()
						+ ")");
				if (i != limit - 1)
					result.append(":");
			}
			context.write(new Text(tag), new Text(result.toString()));
		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopCOntributors <count> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "TopContributors");

		job.setJarByClass(HComputeContribution.class);
		Scan scan1 = new Scan();
		scan1.setCaching(500);
		scan1.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("user_contribution", scan1,
				HbaseMapper.class, Text.class, Text.class, job);

		job.setReducerClass(TopContributorReducer.class);
		k = Integer.parseInt(otherArgs[0]);
		job.setNumReduceTasks(15);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
}
