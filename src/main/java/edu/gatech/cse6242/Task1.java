package edu.gatech.cse6242;

import java.util.*; 

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Task1 {

	public static class EdgesMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> { 

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException { 
			String[] partsStringArray = value.toString().split("\t");
			output.collect(new IntWritable(Integer.parseInt(partsStringArray[1])), 
					new IntWritable(Integer.parseInt(partsStringArray[2])));
		} 
	} 
 
	public static class EdgesReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {  

		public void reduce(IntWritable key, Iterator<IntWritable> values, 
						OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException { 
			int incomeWeightSum = 0;
		    	while (values.hasNext()) {
				incomeWeightSum += values.next().get();
			}
			output.collect(key, new IntWritable(incomeWeightSum));
		 } 
	}  

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task1.class);
		conf.setJobName("Task1");

		conf.setMapperClass(EdgesMapper.class); 
		conf.setCombinerClass(EdgesReducer.class); 
		conf.setReducerClass(EdgesReducer.class); 
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
