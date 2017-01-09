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
package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexDeleteEndRecord extends LogicalEndRecord implements LogRecord {
	private long txNum;
	private String idxName;
	private SearchKey searchKey;
	private RecordId recordId;
	private LogSeqNum lsn;

	public IndexDeleteEndRecord(long txNum, String idxName, SearchKey searchKey, RecordId recordId,
			LogSeqNum logicalStartLSN) {
		this.txNum = txNum;
		this.idxName = idxName;
		this.searchKey = searchKey;
		this.recordId = recordId;
		super.logicalStartLSN = logicalStartLSN;
		this.lsn = null;
	}

	public IndexDeleteEndRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		idxName = (String) rec.nextVal(VARCHAR).asJavaVal();
		
		// num_of_flds (type1, val1, type2, val2 ... typeN, valN)
		List<Constant> vals = new LinkedList<Constant>();
		int numOfFlds = (Integer) rec.nextVal(INTEGER).asJavaVal();
		for (int i = 0; i < numOfFlds; i++) {
			int keyType = (Integer) rec.nextVal(INTEGER).asJavaVal();
			vals.add(rec.nextVal(Type.newInstance(keyType)));
		}
		searchKey = new SearchKey(vals);

		String recordIdBlkFile = (String) rec.nextVal(VARCHAR).asJavaVal();
		long recordIdBlkNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		int recordIdSlotNum = (Integer) rec.nextVal(INTEGER).asJavaVal();
		recordId = new RecordId(new BlockId(recordIdBlkFile, recordIdBlkNum), recordIdSlotNum);
		
		super.logicalStartLSN = new LogSeqNum((Long) rec.nextVal(BIGINT).asJavaVal(),
				(Long) rec.nextVal(BIGINT).asJavaVal());
		lsn = rec.getLSN();
	}

	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_INDEX_FILE_DELETE_END;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {

		Map<String, IndexInfo> iiMap = VanillaDb.catalogMgr().getIndexInfo(tblName, tx);
		BlockId blk = new BlockId(tblName + ".tbl", recordBlockNum);
		RecordId rid = new RecordId(blk, recordSlotId);
		IndexInfo ii = iiMap.get(fldName);
		if (ii != null) {
			Index idx = ii.open(tx);
			idx.insert(searchKey, rid, false);
			idx.close();
		}
		// Append a Logical Abort log at the end of the LogRecords
		LogSeqNum lsn = tx.recoveryMgr().logLogicalAbort(this.txNum,this.logicalStartLSN);
		VanillaDb.logMgr().flush(lsn);

	}

	/**
	 * Logical Record should not be redo since it would not do the same physical
	 * operations as the time it terminated.
	 * 
	 * @see LogRecord#redo(Transaction)
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing

	}

	@Override
	public String toString() {
		return "<INDEX DELETE END " + txNum + " " + idxName + " " + searchKey
				+ " " + recordId + " " + super.logicalStartLSN + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new VarcharConstant(idxName));
		
		// num_of_flds (type1, val1, type2, val2 ... typeN, valN)
		rec.add(new IntegerConstant(searchKey.getNumOfFields()));
		for (int i = 0; i < searchKey.getNumOfFields(); i++) {
			rec.add(new IntegerConstant(searchKey.get(i).getType().getSqlType()));
			rec.add(searchKey.get(i));
		}
		
		rec.add(new VarcharConstant(recordId.block().fileName()));
		rec.add(new BigIntConstant(recordId.block().number()));
		rec.add(new IntegerConstant(recordId.id()));
		rec.add(new BigIntConstant(super.logicalStartLSN.blkNum()));
		rec.add(new BigIntConstant(super.logicalStartLSN.offset()));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}

}
