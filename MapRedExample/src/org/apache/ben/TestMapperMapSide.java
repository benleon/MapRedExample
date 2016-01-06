package org.apache.ben;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestMapperMapSide extends Mapper<LongWritable, Text, Text, Text> {

	// private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
	private Text outValue = new Text();

	private HashMap<String, String> lookup = new HashMap<String, String>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.setup(context);

		URI[] mapsideFiles = context.getCacheFiles();
		FileSystem fs = FileSystem.get(new Configuration());
		
		for (URI u : mapsideFiles) {
			System.out.println("CacheFile" + u.toString());
			String trimmedFile = u.toString().substring(u.toString().indexOf('#') +1);
			System.out.println("trimmedFile" + trimmedFile);

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(trimmedFile))));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] cols = line.split(",");
				System.out.println("Added Lookup " + cols[0] + " value " + line);
				lookup.put(cols[0], line);
				System.out.println(line);
				line = br.readLine();
				
			}

		}

	}
	
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("mapper map map map");
		System.out.println("MapperMapSide" + context.getTaskAttemptID() + " input key " + key + " input value " + value);
		String[] colLeft = value.toString().split(",");
		String rightString = this.lookup.get(colLeft[0]);
		try {
			
			outKey.set(colLeft[0]);
			
			if (rightString == null )
			{
				outValue.set(colLeft[1] + ",,");
			}
			else 
			{
				String[] colRight = rightString.split(",");
				outValue.set(colLeft[1]+ "," + colRight[1] + "," + colRight[2]);
			}
			
      	//  keyOut.set(cols[0]);
      	 // valOut.set(colLeft[1]+ "," + colRight[1] + "," + colRight[2] );
			
			//outValue.set("left" + value.toString());
			System.out.println("MapperOut" + outKey.toString() + " value " + outValue.toString());
			context.write(outKey, outValue);
		} catch (Exception e) {
			System.err.println("Error occured " + e.toString());
			e.printStackTrace();
		}

	}
}