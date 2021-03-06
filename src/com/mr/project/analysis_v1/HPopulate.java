package com.mr.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class HPopulate {

	public static class PostMapper extends Mapper<Object, Text,  ImmutableBytesWritable, Put> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] POST = "posts".getBytes();
		public static final byte[] ID = "id".getBytes();		
		public static final byte[] TYPE = "type".getBytes();
		public static final byte[] PARENTID = "parent-id".getBytes();	
		public static final byte[] CREATIONDATE = "creation-date".getBytes();
		public static final byte[] OWNERUSERID = "owner-user-id".getBytes();
		public static final byte[] TAG = "tag".getBytes();
		
		

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CSVParser parser = new CSVParser(',');
			
			if(value==null || value.toString().isEmpty())
				return;
			String[] data = parser.parseLine(value.toString());
			
			Put put = new Put(Bytes.toBytes(data[0]+","+System.nanoTime()));
						
			put.add(DATA, ID, Bytes.toBytes(data[0]));
			put.add(DATA, TYPE, Bytes.toBytes(data[1]));
			put.add(DATA, PARENTID, Bytes.toBytes(data[2]));
			put.add(DATA, CREATIONDATE, Bytes.toBytes(data[3]));
			put.add(DATA, OWNERUSERID, Bytes.toBytes(data[5]));
			put.add(DATA, TAG, Bytes.toBytes(data[7]));
			
			
			context.write(new ImmutableBytesWritable(POST), put); // write to the posts table

		}

	}

	public static class UserMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] USER = "users".getBytes();
		public static final byte[] USERID = "userid".getBytes();
		public static final byte[] USERNAME = "username".getBytes();
		public static final byte[] CREATIONDATE = "creation-date".getBytes();
		public static final byte[] LOCATION = "location".getBytes();
		public static final byte[] AGE = "age".getBytes();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CSVParser parser = new CSVParser(',');
			
			if(value==null || value.toString().isEmpty())
				return;
			String[] data=null;
			try{
			data = parser.parseLine(value.toString());
			}
			catch (IOException e)
			{
				System.out.println(value.toString());
				return;
			}
			
			Put put = new Put(Bytes.toBytes(Long.parseLong(data[0])));
						
			put.add(DATA, USERID, Bytes.toBytes(data[0]));
			put.add(DATA, CREATIONDATE, Bytes.toBytes(data[2]));
			put.add(DATA, USERNAME, Bytes.toBytes(data[3]));
			put.add(DATA, AGE, Bytes.toBytes(data[5]));
			
			
			context.write(new ImmutableBytesWritable(USER), put); // write to the posts table

		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		// Create table

		Configuration config = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(config, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: HPopulate <user> <post>");
			System.exit(1);
		}

		HBaseAdmin admin = new HBaseAdmin(config);

		if (admin.tableExists("posts")) {
			admin.disableTable("posts");
			admin.deleteTable("posts");
			admin.majorCompact(".META.");
		}
		
		if (admin.tableExists("users")) {
			admin.disableTable("users");
			admin.deleteTable("users");
			admin.majorCompact(".META.");
		}

		HTableDescriptor htd = new HTableDescriptor("posts");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		//admin.createTable(htd);
		admin.createTable(htd);
	

		htd = new HTableDescriptor("users");
		hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		admin.close();

		Job job = new Job(config, "HPopulate");
		job.setJarByClass(HPopulate.class);
		Path userInputPath = new Path(otherArgs[0]);
		Path postInputPath = new Path(otherArgs[1]);
		MultipleInputs.addInputPath(job, postInputPath, TextInputFormat.class,
				PostMapper.class);
		MultipleInputs.addInputPath(job, userInputPath, TextInputFormat.class,
				UserMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setNumReduceTasks(0);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
