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
package org.vanilladb.core.storage.index.hash;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;
import org.vanilladb.core.util.CoreProperties;

/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class HashIndex extends Index {
	/**
	 * A field name of the schema of index records.
	 */
	private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id";

	public static final int NUM_BUCKETS;

	static {
		NUM_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
				HashIndex.class.getName() + ".NUM_BUCKETS", 100);
	}

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
		return (totRecs / rpb) / NUM_BUCKETS;
	}

	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		// XXX: I'am not sure if using "key_0" as the field name of the key
		// is a good design
		for (int i = 0; i < keyType.getNumOfFields(); i++)
			sch.addField(getKeyFldName(i), keyType.get(i));
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}
	
	private static String getKeyFldName(int keyNum) {
		return SCHEMA_KEY + '_' + keyNum;
	}

	private IndexInfo ii;
	private SearchKeyType keyType;
	private String dataFileName;
	private Transaction tx;
	private SearchKey searchKey;
	private RecordFile rf;

	/**
	 * Opens a hash index for the specified index.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param fldType
	 *            the type of the indexed field
	 * @param tx
	 *            the calling transaction
	 */
	public HashIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		this.ii = ii;
		this.dataFileName = ii.tableName() + ".tbl";
		this.keyType = keyType;
		this.tx = tx;
	}

	@Override
	public void preLoadToMemory() {
		for (int i = 0; i < NUM_BUCKETS; i++) {
			String tblname = ii.indexName() + i + ".tbl";
			long size = fileSize(tblname);
			BlockId blk;
			for (int j = 0; j < size; j++) {
				blk = new BlockId(tblname, j);
				tx.bufferMgr().pin(blk);
			}
		}
	}

	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a {@link RecordFile} on the file corresponding to the bucket.
	 * The record file for the previous bucket (if any) is closed.
	 * 
	 * @see Index#beforeFirst(ConstantRange)
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {
		close();
		
		// support the equality query only
		if (!searchRange.isEqualitySearch())
			throw new UnsupportedOperationException();
		searchKey = searchRange.toSearchKey();
		
		int bucket = searchKey.hashCode() % NUM_BUCKETS;
		String tblname = ii.indexName() + bucket;
		TableInfo ti = new TableInfo(tblname, schema(keyType));

		// the underlying record file should not perform logging
		this.rf = ti.open(tx, false);

		// initialize the file header if needed
		if (rf.fileSize() == 0)
			RecordFile.formatFileHeader(ti.fileName(), tx);
		rf.beforeFirst();
	}

	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		while (rf.next()) {
			boolean match = true;
			for (int i = 0; i < searchKey.getNumOfFields(); i++)
				if (rf.getVal(getKeyFldName(i)).compareTo(searchKey.get(i)) != 0) {
					match = false;
					break;
				}
			
			if (match)
				return true;
		}
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(Constant, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
		beforeFirst(key.toSearchRange());
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// insert the data
		rf.insert();
		for (int i = 0; i < key.getNumOfFields(); i++)
			rf.setVal(getKeyFldName(i), key.get(i));
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
				.number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key, 
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(Constant, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
		beforeFirst(key.toSearchRange());
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				return;
			}
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key, 
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (rf != null)
			rf.close();
	}

	private long fileSize(String fileName) {
		try {
			tx.concurrencyMgr().readFile(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		return VanillaDb.fileMgr().size(fileName);
	}
}
