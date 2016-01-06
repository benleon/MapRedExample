package org.apache.ben;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TestMapperRight
       extends Mapper<LongWritable, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	System.out.println("MapperRight" + context.getTaskAttemptID() + " input key " + key + " input value " + value) ;
    	String[] columns = value.toString().split(",");
    	
    	try{
    		outKey.set(columns[0]);
    		outValue.set("right"+value.toString());
    		System.out.println("MapperOut" + outKey.toString() + " value " + outValue.toString());
    		context.write(outKey, outValue);
    	} catch (Exception e)
    	{
    		System.err.println("Error occured " + e.toString());
    		e.printStackTrace();
    	}
      
    }
  }