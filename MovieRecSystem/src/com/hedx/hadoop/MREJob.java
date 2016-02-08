package com.hedx.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MREJob {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 3) {
	      System.err.println("Usage: MREJob <in> <tempop> <out>");
	      System.exit(2);
	    }
	    ControlledJob cJob1 = new ControlledJob(conf);
	    
		Job job = Job.getInstance(conf, "MRE Job1");
		cJob1.setJob(job);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setJarByClass(MREJob.class);
		job.setMapperClass(MREMapper1.class);
		job.setReducerClass(MREReducer1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		ControlledJob cJob2 = new ControlledJob(conf);
		Job job2 = Job.getInstance(conf, "MRE Job2");
		cJob2.setJob(job2);
		
		Path in2 = new Path(args[1]);
		Path out2 = new Path(args[2]);
		
		FileInputFormat.addInputPath(job2, in2);
		FileOutputFormat.setOutputPath(job2, out2);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.setJarByClass(MREJob.class);
		job2.setMapperClass(MREMapper2.class);
		job2.setReducerClass(MREReducer2.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		JobControl jobControl = new JobControl("MRE Job Control");
		jobControl.addJob(cJob1);
		jobControl.addJob(cJob2);
		cJob2.addDependingJob(cJob1);

		
		//jobControl.run();
		Thread thread = new Thread(jobControl);
        thread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(10000);
        }
        System.exit(0);
		
		//System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
