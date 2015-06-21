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

public class HComputeContributionV2 {

	public static class HbaseMapper extends TableMapper<LongWritable, Text> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] OWNERUSERID = "owner-user-id".getBytes();
		public static final byte[] TAG = "tag".getBytes();
		public static final byte[] FLAG = "flag".getBytes();
		public static final byte[] USERNAME = "username".getBytes();

		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws InterruptedException, IOException {
			String rowKey=Bytes.toString(row.get());
			String[] data=rowKey.split(",");
			
			String flag = data[1];
			if (flag.equalsIgnoreCase("post")) {
				String ownerUserId = new String(value.getValue(DATA,
						OWNERUSERID));
				String tag = new String(value.getValue(DATA, TAG));

				context.write(new LongWritable(Long.parseLong(ownerUserId)),
						new Text("posts," + tag));
			} else {
//				String[] data = Bytes.toString(row.get()).split(",");
				String userId = data[0];
//				String userId = new String(
//						value.getValue(DATA, USERID));
				String ownerUserName = new String(
						value.getValue(DATA, USERNAME));

				context.write(new LongWritable(Long.parseLong(userId)),
						new Text("user," + ownerUserName));
			}
		}
	}

	public static class HBaseReducer extends
	TableReducer<LongWritable, Text, Text> {
public static final byte[] DATA = "data".getBytes();
public static final byte[] COUNT = "count".getBytes();
public static final byte[] TAG = "tag".getBytes();
public static final byte[] USERNAME = "username".getBytes();

public void reduce(LongWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	Map<String, Long> userContributions = new HashMap<String, Long>();
	String userName = "";
	for (Text val : values) {
		String[] data = val.toString().split(",");
		if (data[0].equalsIgnoreCase("user")) {
			userName = data[1];
		} else {
			String tag = data[1];
			Long value = userContributions.containsKey(tag) ? userContributions
					.get(tag) : 0L;
			value++;
			userContributions.put(tag, value);
		}
	}
//	if(userContributions.isEmpty())
//		System.out.println("No posts for user :"+userName);

	for (Entry<String, Long> entry : userContributions.entrySet()) {
		Put put = new Put(Bytes.toBytes(key.toString()+","+System.nanoTime()));

		put.add(DATA, USERNAME, Bytes.toBytes(userName));
		put.add(DATA, TAG, Bytes.toBytes(new String(entry.getKey())));
		put.add(DATA, COUNT, Bytes.toBytes(new Long(entry.getValue())));
		//System.out.println(entry.getValue());
		context.write(null, put);
	}

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
		job.setJarByClass(HComputeContributionV2.class);

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
		job.setNumReduceTasks(15);
		//
		TableMapReduceUtil.initTableReducerJob("user_contribution",
				HBaseReducer.class, job);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
