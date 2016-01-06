package org.apache.ben;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class TestReducerJoin
       extends Reducer<Text,Text,Text,Text> {
    private Text valOut = new Text();
    private Text keyOut = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	System.out.println("Reducer" + context.getTaskAttemptID() + " input key " + key + " input value " + values.toString()) ;

      ArrayList<String> left = new ArrayList<String>();
      ArrayList<String> right = new ArrayList<String>();
      for (Text val : values) {
      	System.out.println("One Value" + context.getTaskAttemptID() + " " + val) ;
      	String currentRow = val.toString();
      	if (currentRow.startsWith("left"))
      	{
      		left.add(currentRow.substring(4));
      	}
      	if (currentRow.startsWith("right"))
      	{
      		right.add(currentRow.substring(5));
      	}
       
      }
      for (String l: left)
      {
    	  String[] colLeft = l.split(",");
    	  
    	  for (String r: right)
    	  {
    		  System.out.println("Join" + l + " r " + r );

        	  String[] colRight= r.split(",");
        	  keyOut.set(colLeft[0]);
        	  valOut.set(colLeft[1]+ "," + colRight[1] + "," + colRight[2] );
        	  
    		  context.write(keyOut,valOut);
    	  }
      }
     // context.write(key, result);
     
    }
    
  }