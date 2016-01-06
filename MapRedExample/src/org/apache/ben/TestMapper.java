package org.apache.ben;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TestMapper
       extends Mapper<LongWritable, Text, TestCombinedWritable, LongWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private TestCombinedWritable outKey = new TestCombinedWritable();
    private LongWritable outValue = new LongWritable(0);

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	System.out.println("Mapper" + context.getTaskAttemptID() + " input key " + key + " input value " + value) ;
    	String[] columns = value.toString().split(",");
    	int data = 0;
    	try{
    		data = Integer.valueOf(columns[2]);
    		outKey.name = columns[0];
    		outKey.year = columns[1];
    		//outKey.set(columns[0]);
    		outValue.set(data);
    		System.out.println("MapperOut" + outKey.toString() + " value " + outValue.toString());
    		context.write(outKey, outValue);
    	} catch (Exception e)
    	{
    		System.err.println("Error occured " + e.toString());
    		e.printStackTrace();
    	}
      
    }
  }