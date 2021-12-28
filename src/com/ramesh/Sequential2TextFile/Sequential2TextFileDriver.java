package com.ramesh.Sequential2TextFile;



	import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 
	public class Sequential2TextFileDriver
	{
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
 
			args = new String[] { 
					"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/3.MapOnlyJob-Sequential2TextFile/input_data/input.txt",
					"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/3.MapOnlyJob-Sequential2TextFile/output_data/"};
					 
					/* delete the output directory before running the job */
					FileUtils.deleteDirectory(new File(args[1])); 
					 
					if (args.length != 2) {
					System.err.println("Please specify the input and output path");
					System.exit(-1);
					}
					
					System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
					
					Configuration conf = new Configuration();
					Job sampleJob = new Job(conf,"Sequential file 2 text");
		
 
					sampleJob.setJarByClass(Sequential2TextFileDriver.class);
 
			
			//Add input and output file paths to job based on the arguments passed
			FileInputFormat.addInputPath(sampleJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(sampleJob, new Path(args[1]));
		
			sampleJob.setOutputKeyClass(Text.class);
			sampleJob.setOutputValueClass(Text.class);
		
			sampleJob.setInputFormatClass(SequenceFileInputFormat.class);
			sampleJob.setOutputFormatClass(TextOutputFormat.class);
			
			//Set the MapClass and ReduceClass in the job
			sampleJob.setMapperClass(Sequential2TextFileMapper.class);
			
			//Setting the number of reducer tasks to 0 as we do not 
			//have any reduce tasks in this example. We are only concentrating on the Mapper
			sampleJob.setNumReduceTasks(0);
			
			
			//Wait for the job to complete and print if the job was successful or not
			int returnValue = sampleJob.waitForCompletion(true) ? 0:1;
			
			if(sampleJob.isSuccessful()) {
				System.out.println("Job was successful");
			} else if(!sampleJob.isSuccessful()) {
				System.out.println("Job was not successful");			
			} 
		}
	}