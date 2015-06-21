package com.mr.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HComputeContribution {

	public static class HbaseMapper extends TableMapper<LongWritable, Text> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] OWNERUSERID = "owner-user-id".getBytes();
		public static final byte[] TAG = "tag".getBytes();

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws InterruptedException, IOException {

			String ownerUserId = new String(value.getValue(DATA, OWNERUSERID));
			String tag = new String(value.getValue(DATA, TAG));

			context.write(new LongWritable(Long.parseLong(ownerUserId)),
					new Text("posts," + tag));
		}
	}

	public static class HBaseReducer extends
			TableReducer<LongWritable, Text, Text> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] COUNT = "count".getBytes();
		public static final byte[] TAG = "tag".getBytes();
		public static final byte[] USERNAME = "username".getBytes();
		public static HTable hTable;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = HBaseConfiguration.create();
			hTable = new HTable(conf, "users");
		}

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Get g = new Get(Bytes.toBytes(key.get()));
			Result result = hTable.get(g);
			if(result.getValue(DATA, USERNAME)==null)
				return;
			String userName = new String(result.getValue(DATA, USERNAME));

			Map<String, Long> userContributions = new HashMap<String, Long>();

			for (Text val : values) {
				String[] data = val.toString().split(",");
				String tag = data[1];
				Long value = userContributions.containsKey(tag) ? userContributions
						.get(tag) : 0L;
				value++;
				userContributions.put(tag, value);
				

			}

			for (Entry<String, Long> entry : userContributions.entrySet()) {
				Put put = new Put(Bytes.toBytes(key.toString() + ","
						+ System.nanoTime()));

				put.add(DATA, USERNAME, Bytes.toBytes(userName));
				put.add(DATA, TAG, Bytes.toBytes(new String(entry.getKey())));
				put.add(DATA, COUNT, Bytes.toBytes(new Long(entry.getValue())));
				context.write(null, put);
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			hTable.close();
		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists("user_contribution")) {
			admin.disableTable("user_contribution");
			admin.deleteTable("user_contribution");
			admin.majorCompact(".META.");
		}

		HTableDescriptor htd = new HTableDescriptor("user_contribution");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		admin.close();

		Job job = new Job(config, "HComputeContribution");
		job.setJarByClass(HComputeContribution.class);

		Scan scan1 = new Scan();
		scan1.setCaching(500);
		scan1.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob("posts", // input HBase table name
				scan1, // Scan instance to control CF and attribute selection
				HbaseMapper.class, // mapper
				null, // mapper output key
				null, // mapper output value
				job);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		//
		TableMapReduceUtil.initTableReducerJob("user_contribution",
				HBaseReducer.class, job);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
