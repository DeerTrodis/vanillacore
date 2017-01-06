/*******************************************************************************
 * Copyright 2016 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.index;

import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.index.btree.BTreeIndex;
import org.vanilladb.core.storage.index.hash.HashIndex;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * An abstract index that defines the index traversal interface and provides
 * type-agnostic methods.
 */
public abstract class Index {
	/**
	 * Estimates the number of block accesses required to find all index records
	 * matching a search range, given the specified numbers of total records and
	 * matching records.
	 * <p>
	 * This number does <em>not</em> include the block accesses required to
	 * retrieve data records.
	 * </p>
	 * 
	 * @param idxType
	 *            the index type
	 * @param keyTypes
	 *            the list of types of the indexed fields
	 * @param totRecs
	 *            the total number of records in the table
	 * @param matchRecs
	 *            the number of matching records
	 * @return the estimated the number of block accesses
	 */
	public static long searchCost(IndexType idxType, List<Type> keyTypes, long totRecs, long matchRecs) {
		if (idxType == IndexType.HASH)
			return HashIndex.searchCost(keyTypes, totRecs, matchRecs);
		else if (idxType == IndexType.BTREE)
			return BTreeIndex.searchCost(keyTypes, totRecs, matchRecs);
		else
			throw new IllegalArgumentException("unsupported index type");
	}

	/**
	 * Create an instance of indices with the specified {@link IndexInfo}.
	 * 
	 * @param ii
	 *            the information of the specified index
	 * @param tx
	 *            the calling transaction
	 * @return
	 *            the new instance of the index
	 */
	public static Index newInstance(IndexInfo ii, List<Type> keyTypes, Transaction tx) {
		if (ii.indexType() == IndexType.HASH)
			return new HashIndex(ii, keyTypes, tx);
		else if (ii.indexType() == IndexType.BTREE)
			return new BTreeIndex(ii, keyTypes, tx);
		else
			throw new IllegalArgumentException("unsupported index type");
	}

	/**
	 * Positions the index before the first index record matching the specified
	 * range of search keys.
	 * 
	 * @param searchRange
	 *            the list of ranges of search keys
	 */
	public abstract void beforeFirst(List<ConstantRange> searchRanges);

	/**
	 * Moves the index to the next record matching the search range specified in
	 * the {@link #beforeFirst} method. Returns false if there are no more such
	 * index records.
	 * 
	 * @return false if no other index records for the search range.
	 */
	public abstract boolean next();

	/**
	 * Returns the data record ID stored in the current index record.
	 * 
	 * @return the data record ID stored in the current index record.
	 */
	public abstract RecordId getDataRecordId();

	/**
	 * Inserts an index record having the specified key and data record ID.
	 * 
	 * @param keys
	 *            the list of indexed keys in the new index record.
	 * @param dataRecordId
	 *            the data record ID in the new index record.
	 */
	public abstract void insert(List<Constant> keys, RecordId dataRecordId, boolean doLogicalLogging);

	/**
	 * Deletes the index record having the specified key and data record ID.
	 * 
	 * @param keys
	 *            the list of indexed keys in the new index record.
	 * @param dataRecordId
	 *            the data record ID of the deleted index record
	 */
	public abstract void delete(List<Constant> keys, RecordId dataRecordId, boolean doLogicalLogging);

	/**
	 * Closes the index.
	 */
	public abstract void close();

	/**
	 * Preload the index blocks to memory.
	 */
	public abstract void preLoadToMemory();
}
