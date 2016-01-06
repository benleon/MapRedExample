package org.apache.ben;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class TestReducer
       extends Reducer<TestCombinedWritable,LongWritable,TestCombinedWritable,LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(TestCombinedWritable key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	System.out.println("Reducer" + context.getTaskAttemptID() + " input key " + key + " input value " + values.toString()) ;

      int sum = 0;
      for (LongWritable val : values) {
      	System.out.println("One Value" + context.getTaskAttemptID() + " " + val) ;

        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }