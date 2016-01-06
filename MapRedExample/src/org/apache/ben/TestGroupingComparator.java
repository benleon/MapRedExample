package org.apache.ben;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TestGroupingComparator extends WritableComparator{
	
	protected TestGroupingComparator()
	{
		super(TestCombinedWritable.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2) {
      TestCombinedWritable k1 = (TestCombinedWritable)w1;
      TestCombinedWritable k2 = (TestCombinedWritable)w2;
      
      
      return k1.name.compareTo(k2.name);
  }

}
//public class NaturalKeyGroupingComparator extends WritableComparator {
//    protected NaturalKeyGroupingComparator() {
//        super(StockKey.class, true);
//    }   
//    @SuppressWarnings("rawtypes")
//    @Override
//    public int compare(WritableComparable w1, WritableComparable w2) {
//        StockKey k1 = (StockKey)w1;
//        StockKey k2 = (StockKey)w2;
//         
//        return k1.getSymbol().compareTo(k2.getSymbol());
//    }
//}