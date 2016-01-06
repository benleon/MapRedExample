package org.apache.ben;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TestPartitioner extends Partitioner<TestCombinedWritable, LongWritable> {



	@Override
	public int getPartition(TestCombinedWritable arg0, LongWritable arg1, int arg2) {
		// TODO Auto-generated method stub
		return 1;
	}

}
