package org.vanilladb.core.storage.index;

import java.util.Iterator;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

public class SearchKeyType {
	
	private Type[] types;
	
	public SearchKeyType(List<Type> types) {
		this(types.iterator(), types.size());
	}
	
	public SearchKeyType(Iterator<Type> typeIter, int numOfFields) {
		types = new Type[numOfFields];
		for (int i = 0; i < numOfFields; i++)
			types[i] = typeIter.next();
	}
	
	public Type get(int index) {
		return types[index];
	}
	
	public int getNumOfFields() {
		return types.length;
	}
	
	public SearchKey getMinValue() {
		return new SearchKey(new Iterator<Constant>() {
			
			private int count = 0;

			@Override
			public boolean hasNext() {
				return count < types.length;
			}

			@Override
			public Constant next() {
				Constant val = types[count].minValue();
				count++;
				return val;
			}
			
		}, types.length);
	}
}
