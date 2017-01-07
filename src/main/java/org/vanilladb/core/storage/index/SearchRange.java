package org.vanilladb.core.storage.index;

import java.util.Iterator;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;

public class SearchRange {
	
	private ConstantRange[] ranges;
	
	public SearchRange(List<ConstantRange> ranges) {
		this(ranges.iterator(), ranges.size());
	}
	
	public SearchRange(Iterator<ConstantRange> rangeIter, int numOfFields) {
		ranges = new ConstantRange[numOfFields];
		for (int i = 0; i < numOfFields; i++)
			ranges[i] = rangeIter.next();
	}
	
	public ConstantRange get(int index) {
		return ranges[index];
	}
	
	public boolean isEqualitySearch() {
		for (ConstantRange range : ranges) {
			if (!range.isConstant())
				return false;
		}
		return true;
	}
	
	public boolean isValid() {
		for (ConstantRange range : ranges) {
			if (!range.isValid())
				return false;
		}
		return true;
	}
	
	public SearchKey toSearchKey() {
		return new SearchKey(new Iterator<Constant>() {
			
			private int count = 0;

			@Override
			public boolean hasNext() {
				return count < ranges.length;
			}

			@Override
			public Constant next() {
				Constant val = ranges[count].asConstant();
				count++;
				return val;
			}
			
		}, ranges.length);
	}
}
