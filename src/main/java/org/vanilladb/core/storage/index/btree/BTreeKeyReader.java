package org.vanilladb.core.storage.index.btree;

import java.util.Iterator;

import org.vanilladb.core.sql.Constant;

class BTreeKeyReader implements Iterator<Constant> {
	
	private static final String SCH_KEY = "key";
	
	static String getKeyFldName(int keyNum) {
		return SCH_KEY + '_' + keyNum;
	}
	
	private BTreePage page;
	private int slot;
	private int numOfFlds;
	private int currentIndex = 0;
	
	BTreeKeyReader(BTreePage page, int slot, int numOfFlds) {
		this.page = page;
		this.slot = slot;
		this.numOfFlds = numOfFlds;
	}
	
	@Override
	public boolean hasNext() {
		return currentIndex < numOfFlds;
	}

	@Override
	public Constant next() {
		Constant val = page.getVal(slot, getKeyFldName(currentIndex));
		currentIndex++;
		return val;
	}
	
}
