package org.apache.ben;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class TestCombinedWritable  implements WritableComparable<TestCombinedWritable>{
	public String name;
	public String year;
	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		year = in.readUTF();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
	    out.writeUTF(year);
		
	}
	@Override
	public int compareTo(TestCombinedWritable o) {
		return ComparisonChain.start().compare(name, o.name).compare(year, o.year).result();
	}
	
	public String toString()
	{
		return this.name + "-" + this.year;
	}
	
	

}
