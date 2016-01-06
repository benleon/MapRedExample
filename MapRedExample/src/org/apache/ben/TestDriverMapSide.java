package org.apache.ben;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestDriverMapSide {

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    //conf.set("mapred.reduce.tasks", "0");
    Job job = Job.getInstance(conf, "bentestMapSideJoin");
    job.setNumReduceTasks(0);
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] fileStatus = fs.listStatus(new Path(args[1]));
    //File folder = new File(args[1]);
   // File[] listOfFiles = folder.listFiles();
    for (FileStatus status : fileStatus) {
        if (status.isFile()) {
        	System.out.println(status.getPath().toString());
        	 job.addCacheFile(new URI(status.getPath().toString()+ "#" + status.getPath().getName()));
        }
    }
    //job.addCacheFile(new URI(“/user/hadoop/joinProject/data/departments_txt”));
    //DistributedCache.addCacheFile(new URI(“/user/hadoop/joinProject/data/departments_txt”),conf);
    //job.setCacheFiles(files);
    //job.setJarByClass(TestDriver.class);
    //MultipleInputs.
    job.setMapperClass(TestMapperMapSide.class);
   // job.setCombinerClass(TestReducer.class);
   // job.setReducerClass(TestReducerJoin.class);
   // job.setPartitionerClass(TestPartitioner.class);
   // job.setMapOutputKeyClass(TestCombinedWritable.class);
   // job.setMapOutputValueClass(LongWritable.class);
   // job.setGroupingComparatorClass(TestGroupingComparator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //job.setIn.addInputPath(job, new Path(args[0]), TextInputFormat.class, TestMapperLeft.class);
   // MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TestMapperRight.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setJar("file:///home/hdfs/MapRedTest/test7.jar");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}