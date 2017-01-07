package org.vanilladb.core.storage.index;

import java.util.Iterator;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;

public class SearchKey {
	
	private Constant[] vals;
	
	public SearchKey(List<Constant> values) {
		this(values.iterator(), values.size());
	}
	
	public SearchKey(Iterator<Constant> valIter, int numOfFields) {
		vals = new Constant[numOfFields];
		for (int i = 0; i < numOfFields; i++)
			vals[i] = valIter.next();
	}
	
	public Constant get(int index) {
		return vals[index];
	}
	
	public int getNumOfFields() {
		return vals.length;
	}
	
	public SearchRange toSearchRange() {
		return new SearchRange(new Iterator<ConstantRange>() {
			
			private int count = 0;

			@Override
			public boolean hasNext() {
				return count < vals.length;
			}

			@Override
			public ConstantRange next() {
				ConstantRange range = ConstantRange.newInstance(vals[count]);
				count++;
				return range;
			}
			
		}, vals.length);
	}
	
	/**
	 * Compare the list of {@link ConstantRange} to this key. If the first value of the key is less than
	 * or larger than the corresponding search range, it will terminate the comparison, then return the 
	 * result. If the value contains in the range, it will continue to compare the next key until there is
	 * a value that doesn't contains in the corresponding range. 
	 * 
	 * @param ranges
	 *            the list of {@link ConstantRange} for comparison
	 * 
	 * @return -1, 0 or 1 as the key less than, contains, or larger than the ranges
	 */
	public int compareTo(SearchRange searchRange) {
		// XXX: Maybe we should check if they have the same number of values ?
		for (int i = 0; i < vals.length; i++) {
			ConstantRange range = searchRange.get(i);
			Constant val = vals[i];
			
			if (range.largerThan(val)) // val < range
				return -1;
			else if (range.lessThan(val)) // val > range
				return 1;
		}
		return 0;
	}
	
	// TODO: Document
	public int compareTo(SearchKey target) {
		// XXX: Maybe we should check if they have the same number of values ?
		for (int i = 0; i < vals.length; i++) {
			Constant selfVal = vals[i];
			Constant targetVal = target.vals[i];
			
			int result = selfVal.compareTo(targetVal);
			if (result != 0)
				return result;
		}
		return 0;
	}
	
	// TODO: We should override hashcode() as well
//	@Override
//	public boolean equals(Object o) {
//		if (o == this)
//			return true;
//		
//		if (o.getClass().equals(this.getClass()))
//			return false;
//		
//		SearchKey targetKey = (SearchKey) o;
//		return this.compareTo(targetKey) == 0;
//	}
}
