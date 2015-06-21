package com.mr.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class HPopulateV2 {

	public static class PostMapper extends Mapper<Object, Text,  ImmutableBytesWritable, Put> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] POST = "posts".getBytes();
		public static final byte[] FLAG = "flag".getBytes();
		public static final byte[] ID = "id".getBytes();		
		public static final byte[] TYPE = "type".getBytes();
		public static final byte[] PARENTID = "parent-id".getBytes();	
		public static final byte[] CREATIONDATE = "creation-date".getBytes();
		public static final byte[] OWNERUSERID = "owner-user-id".getBytes();
		public static final byte[] TAG = "tag".getBytes();
		public static HTable hTable;
		List<Put> puts;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			puts=new ArrayList<Put>();
			Configuration conf = HBaseConfiguration.create();
			hTable = new HTable(conf, "posts");
			hTable.setWriteBufferSize(1024*1024*24);
			hTable.setAutoFlush(false);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CSVParser parser = new CSVParser(',');
			
			if(value==null || value.toString().isEmpty())
				return;
			String[] data = parser.parseLine(value.toString());
			
			Put put = new Put(Bytes.toBytes(data[0]+","+"post"+","+System.nanoTime()));
						
			put.add(DATA, ID, Bytes.toBytes(data[0]));
			put.add(DATA, TYPE, Bytes.toBytes(data[1]));
			put.add(DATA, PARENTID, Bytes.toBytes(data[2]));
			put.add(DATA, CREATIONDATE, Bytes.toBytes(data[3]));
			put.add(DATA, OWNERUSERID, Bytes.toBytes(data[5]));
			put.add(DATA, TAG, Bytes.toBytes(data[7]));
			puts.add(put);
			if(puts.size()>500)
			{
				hTable.getWriteBuffer().addAll(puts);
				hTable.flushCommits();
				puts.clear();
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			hTable.getWriteBuffer().addAll(puts);
			hTable.flushCommits();
			puts.clear();
			hTable.close();
		}

	}

	public static class UserMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
		public static final byte[] DATA = "data".getBytes();
		public static final byte[] FLAG = "flag".getBytes();
		public static final byte[] POST = "posts".getBytes();
		public static final byte[] USERID = "userid".getBytes();
		public static final byte[] USERNAME = "username".getBytes();
		public static final byte[] CREATIONDATE = "creation-date".getBytes();
		public static final byte[] LOCATION = "location".getBytes();
		public static final byte[] AGE = "age".getBytes();
		public static HTable hTable;
		List<Put> puts;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			puts=new ArrayList<Put>();
			Configuration conf = HBaseConfiguration.create();
			hTable = new HTable(conf, "posts");
			hTable.setWriteBufferSize(1024*1024*24);
			hTable.setAutoFlush(false);
		}

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
			
			Put put = new Put(Bytes.toBytes(Long.parseLong(data[0])+",user"));
						
			put.add(DATA, USERID, Bytes.toBytes(data[0]));
			//put.add(DATA, FLAG, Bytes.toBytes("user"));
			put.add(DATA, CREATIONDATE, Bytes.toBytes(data[2]));
			put.add(DATA, USERNAME, Bytes.toBytes(data[3]));
			put.add(DATA, AGE, Bytes.toBytes(data[5]));
			
			puts.add(put);
			if(puts.size()>500)
			{
				hTable.getWriteBuffer().addAll(puts);
				hTable.flushCommits();
				puts.clear();
			}
			

		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			hTable.getWriteBuffer().addAll(puts);
			hTable.flushCommits();
			puts.clear();
			puts.clear();
			hTable.close();
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

		HTableDescriptor htd = new HTableDescriptor("posts");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		admin.close();

		Job job = new Job(config, "HPopulateV2");
		job.setJarByClass(HPopulateV3.class);
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
