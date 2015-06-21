package com.mr.project.datacleaning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class PostJoin {

	public static class PostMapper extends
			Mapper<Object, Text, LongWritable, PostValue> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CSVParser parser = new CSVParser(',');
			if(value==null|| value.toString().isEmpty())
				return;
			String[] values = parser.parseLine(value.toString());
			
			PostValue postValue = PostValue.createPost(values);
			if (Long.parseLong(values[2]) != 0) {				
				context.write(postValue.getParentId(), postValue);
			}
			else
				context.write(postValue.getId(), postValue);

		}

	}

	public static class PostReducer extends
			Reducer<LongWritable, PostValue, LongWritable, PostValue> {

		public void reduce(LongWritable key, Iterable<PostValue> values,
				Context context) throws IOException, InterruptedException {
			// change this code
			List<PostValue> posts=new ArrayList<PostValue>();
			PostValue question = null;
			for (PostValue val : values) {
				PostValue postValue = new PostValue(val);
				if (postValue.getParentId().get() == 0)
				{
					question = postValue;
				}		
				posts.add(postValue);
			}
			
			if(question==null)
			{				
				System.out.println("no question for key:"+key);
				return;
			}
			List<String> tags = new ArrayList<String>();
			StringTokenizer tokenizer = new StringTokenizer(question.getTag()
					.toString(),"<>");
			while (tokenizer.hasMoreTokens()) {
				tags.add(tokenizer.nextToken());
			}
			
			for (PostValue val : posts) {
				
				for (String tag : tags) {
					PostValue pv = new PostValue(val);
					pv.setTag(new Text(tag));
					context.write(pv.getId(), pv);
				}
			}

		}

	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Secondary <in> <out>");
			System.exit(2);
		}

		conf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(conf, "PostJoin");

		job.setJarByClass(PostJoin.class);
		job.setMapperClass(PostMapper.class);
		job.setReducerClass(PostReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PostValue.class);
		job.setNumReduceTasks(10);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(PostValue.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
