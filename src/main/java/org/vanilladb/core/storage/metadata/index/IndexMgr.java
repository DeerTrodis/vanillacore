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
package org.vanilladb.core.storage.metadata.index;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.metadata.TableMgr.MAX_NAME;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableMgr;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The index manager. The index manager has similar functionality to the table
 * manager.
 */
public class IndexMgr {
	/**
	 * Name of the index catalog.
	 */
	public static final String ICAT = "idxcat";

	/**
	 * The field names of the index catalog.
	 */
	public static final String ICAT_IDXNAME = "idxname",
			ICAT_TBLNAME = "tblname", ICAT_IDXTYPE = "idxtype";
	
	/**
	 * Name of the key catalog.
	 */
	public static final String KCAT = "idxkeycat";
	
	/**
	 * The field names of the key catalog.
	 */
	public static final String KCAT_IDXNAME = "idxname",
			KCAT_KEYNAME = "keyname";

	private TableInfo iti, kti;

	// Optimization: Materialize the index information
	// Table Name -> (Field Name -> List of IndexInfos which uses the field)
	private Map<String, Map<String, List<IndexInfo>>> iiMap;
	// Index Name -> IndexInfo
	private Map<String, IndexInfo> idxNameMap;

	/**
	 * Creates the index manager. This constructor is called during system
	 * startup. If the database is new, then the <em>idxcat</em> table is
	 * created.
	 * 
	 * @param isNew
	 *            indicates whether this is a new database
	 * @param tx
	 *            the system startup transaction
	 */
	public IndexMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
		if (isNew) {
			Schema sch = new Schema();
			sch.addField(ICAT_IDXNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_TBLNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_IDXTYPE, INTEGER);
			tblMgr.createTable(ICAT, sch, tx);
			
			sch = new Schema();
			sch.addField(KCAT_IDXNAME, VARCHAR(MAX_NAME));
			sch.addField(KCAT_KEYNAME, VARCHAR(MAX_NAME));
			tblMgr.createTable(KCAT, sch, tx);
		}
		iti = tblMgr.getTableInfo(ICAT, tx);
		kti = tblMgr.getTableInfo(KCAT, tx);

		iiMap = new HashMap<String, Map<String, List<IndexInfo>>>();
		idxNameMap = new HashMap<String, IndexInfo>();
	}

	/**
	 * Creates an index of the specified type for the specified field. A unique
	 * ID is assigned to this index, and its information is stored in the idxcat
	 * table.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tblName
	 *            the name of the table
	 * @param fldNames
	 *            the list of names of the indexed fields
	 * @param idxType
	 *            the type of the index
	 * @param tx
	 *            the calling transaction
	 */
	public void createIndex(String idxName, String tblName, List<String> fldNames,
			IndexType idxType, Transaction tx) {
		// Insert the index name, type and the table name into the index catalog
		RecordFile rf = iti.open(tx, true);
		rf.insert();
		rf.setVal(ICAT_IDXNAME, new VarcharConstant(idxName));
		rf.setVal(ICAT_TBLNAME, new VarcharConstant(tblName));
		rf.setVal(ICAT_IDXTYPE, new IntegerConstant(idxType.toInteger()));
		rf.close();
		
		// Insert the keys of the index into the key catalog
		rf = kti.open(tx, true);
		for (String fldName : fldNames) {
			rf.insert();
			rf.setVal(KCAT_IDXNAME, new VarcharConstant(idxName));
			rf.setVal(KCAT_KEYNAME, new VarcharConstant(fldName));
		}
		rf.close();

		// update index info map
		Map<String, List<IndexInfo>> fldToIndexMap = iiMap.get(tblName);
		IndexInfo ii = new IndexInfo(idxName, tblName, fldNames, idxType);
		for (String fld : fldNames) {
			List<IndexInfo> iis = fldToIndexMap.get(fld);
			if (iis == null) {
				iis = new LinkedList<IndexInfo>();
				fldToIndexMap.put(fld, iis);
			}
			iis.add(ii);
		}
	}

	/**
	 * Returns the list of IndexInfo of the indices which index the specified field on the
	 * specified table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param fldName
	 *            the name of index field
	 * @param tx
	 *            the calling transaction
	 * @return a list of IndexInfo objects
	 */
	public List<IndexInfo> getIndexInfo(String tblName, String fldName, Transaction tx) {
		Map<String, List<IndexInfo>> fldToIndexMap = iiMap.get(tblName);
		if (fldToIndexMap != null)
			return fldToIndexMap.get(fldName);
		
		// Retrieve the index names and types
		Map<String, IndexType> idxTypeMap = new HashMap<String, IndexType>();
		RecordFile rf = iti.open(tx, true);
		rf.beforeFirst();
		while (rf.next())
			if (((String) rf.getVal(ICAT_TBLNAME).asJavaVal()).equals(tblName)) {
				String idxName = (String) rf.getVal(ICAT_IDXNAME).asJavaVal();
				int idxType = (Integer) rf.getVal(ICAT_IDXTYPE).asJavaVal();
				idxTypeMap.put(idxName, IndexType.fromInteger(idxType));
			}
		rf.close();
		
		// Retrieve the names of keys
		Map<String, List<String>> fldNamesMap = new HashMap<String, List<String>>();
		rf = kti.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			String idxName = (String) rf.getVal(KCAT_IDXNAME).asJavaVal();
			if (idxTypeMap.keySet().contains(idxName)) {
				List<String> fldNames = fldNamesMap.get(idxName);
				if (fldNames == null) {
					fldNames = new LinkedList<String>();
					fldNamesMap.put(idxName, fldNames);
				}
				fldNames.add((String) rf.getVal(KCAT_KEYNAME).asJavaVal());
			}
		}
		
		/*
		 * Optimization: store the ii. WARNING: if allowing run-time index
		 * schema modification, this opt should be aware of the changing.
		 */
		fldToIndexMap = new HashMap<String, List<IndexInfo>>();
		for (Entry<String, IndexType> entry : idxTypeMap.entrySet()) {
			String idxName = entry.getKey();
			IndexType idxType = entry.getValue();
			List<String> fldNames = fldNamesMap.get(idxName);
			IndexInfo ii = new IndexInfo(idxName, tblName, fldNames, idxType);
			for (String fld : fldNames) {
				List<IndexInfo> iis = fldToIndexMap.get(fld);
				if (iis == null) {
					iis = new LinkedList<IndexInfo>();
					fldToIndexMap.put(fld, iis);
				}
				iis.add(ii);
			}
		}
		iiMap.put(tblName, fldToIndexMap);
		
		return fldToIndexMap.get(fldName);
	}
	
	public IndexInfo getIndexInfo(String idxName, Transaction tx) {
		IndexInfo ii = idxNameMap.get(idxName);
		if (ii != null)
			return ii;
		
		// The information for an IndexInfo
		String tblName = null;
		List<String> fldNames = new LinkedList<String>();
		IndexType idxType = IndexType.BTREE;
		
		// Retrieve the index names and types
		RecordFile rf = iti.open(tx, true);
		rf.beforeFirst();
		while (rf.next())
			if (((String) rf.getVal(ICAT_IDXNAME).asJavaVal()).equals(idxName)) {
				tblName = (String) rf.getVal(ICAT_TBLNAME).asJavaVal();
				int idxTypeNum = (Integer) rf.getVal(ICAT_IDXTYPE).asJavaVal();
				idxType = IndexType.fromInteger(idxTypeNum);
				break;
			}
		rf.close();
		
		// Retrieve the names of keys
		rf = kti.open(tx, true);
		rf.beforeFirst();
		while (rf.next()) {
			if (((String) rf.getVal(KCAT_IDXNAME).asJavaVal()).equals(idxName))
				fldNames.add((String) rf.getVal(KCAT_KEYNAME).asJavaVal());
		}
		
		/*
		 * Optimization: store the ii. WARNING: if allowing run-time index
		 * schema modification, this opt should be aware of the changing.
		 */
		if (tblName != null) {
			ii = new IndexInfo(idxName, tblName, fldNames, idxType);
			idxNameMap.put(idxName, ii);
			// TODO: Add this IndexInfo to iiMap
		}
		
		return ii;
	}
}
