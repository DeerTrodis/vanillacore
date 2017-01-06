package org.vanilladb.core.storage.index.btree;

import java.util.Iterator;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;

/**
 * A key of a B-Tree.
 */
class BTreeKey {
	
	private List<Constant> vals;
	
	BTreeKey(List<Constant> values) {
		this.vals = values;
	}
	
	Iterator<Constant> getIterator() {
		return vals.iterator();
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
	int compareTo(List<ConstantRange> ranges) {
		Iterator<ConstantRange> rangeIter = ranges.iterator();
		Iterator<Constant> valIter = vals.iterator();
		
		while (rangeIter.hasNext()) {
			ConstantRange range = rangeIter.next();
			Constant val = valIter.next();
			
			if (range.largerThan(val)) // val < range
				return -1;
			else if (range.lessThan(val)) // val > range
				return 1;
		}
		return 0;
	}
	
	// TODO: Document
	int compareTo(BTreeKey target) {
		// XXX: Maybe we should check if they have the same number of values ?
		Iterator<Constant> selfIter = vals.iterator();
		Iterator<Constant> targetIter = target.vals.iterator();
		
		while (selfIter.hasNext()) {
			Constant selfVal = selfIter.next();
			Constant targetVal = targetIter.next();
			
			int result = selfVal.compareTo(targetVal);
			if (result != 0)
				return result;
		}
		return 0;
	}
	
	// TODO: We should override hashcode() as well
	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		
		if (o.getClass().equals(this.getClass()))
			return false;
		
		BTreeKey targetKey = (BTreeKey) o;
		return this.compareTo(targetKey) == 0;
	}
}
