package org.apache.ben;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestDriver {

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    conf.set("mapred.reduce.tasks", "2");
    Job job = Job.getInstance(conf, "bentest");
    //job.setCacheFiles(files);
    //job.setJarByClass(TestDriver.class);
    job.setMapperClass(TestMapper.class);
    job.setCombinerClass(TestReducer.class);
    job.setReducerClass(TestReducer.class);
    job.setPartitionerClass(TestPartitioner.class);
   // job.setMapOutputKeyClass(TestCombinedWritable.class);
   // job.setMapOutputValueClass(LongWritable.class);
    job.setGroupingComparatorClass(TestGroupingComparator.class);
    job.setOutputKeyClass(TestCombinedWritable.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJar("file:///home/hdfs/MapRedTest/test7.jar");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}