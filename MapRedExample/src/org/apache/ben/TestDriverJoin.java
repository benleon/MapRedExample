package org.apache.ben;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestDriverJoin {

 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    conf.set("mapred.reduce.tasks", "2");
    Job job = Job.getInstance(conf, "bentestJoin");
    //job.setCacheFiles(files);
    //job.setJarByClass(TestDriver.class);
    //MultipleInputs.
   // job.setMapperClass(TestMapper.class);
   // job.setCombinerClass(TestReducer.class);
    job.setReducerClass(TestReducerJoin.class);
   // job.setPartitionerClass(TestPartitioner.class);
   // job.setMapOutputKeyClass(TestCombinedWritable.class);
   // job.setMapOutputValueClass(LongWritable.class);
   // job.setGroupingComparatorClass(TestGroupingComparator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TestMapperLeft.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TestMapperRight.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setJar("file:///home/hdfs/MapRedTest/test7.jar");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}