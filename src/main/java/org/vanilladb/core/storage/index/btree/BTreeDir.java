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
package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;

/**
 * A B-tree directory page that iterates over the B-tree directory blocks in a
 * file.
 * <p>
 * There is one flag in each B-tree directory block: the level (starting from 0
 * at the deepest) of that block in the directory.
 * </p>
 */
public class BTreeDir {
	/**
	 * A field name of the schema of B-tree directory records.
	 */
	static final String SCH_CHILD = "child";

	static final int NUM_FLAGS = 1;

	private static final String FILENAME_POSTFIX = "_dir.idx";

	public enum SearchPurpose {
		READ, INSERT, DELETE
	};

	public static void insertASlot(Transaction tx, String indexFileName, SearchKeyType keyType, long blkNum, int slotId) {
		// Open the specified directory
		BlockId blk = new BlockId(indexFileName, blkNum);
		BTreeDir dir = new BTreeDir(blk, keyType, tx);

		// Insert the specified slot
		dir.currentPage.insert(slotId);

		// Close the directory
		dir.close();
	}

	public static void deleteASlot(Transaction tx, String indexFileName, SearchKeyType keyType, long blkNum, int slotId) {
		// Open the specified directory
		BlockId blk = new BlockId(indexFileName, blkNum);
		BTreeDir dir = new BTreeDir(blk, keyType, tx);

		// Delete the specified slot
		dir.currentPage.delete(slotId);

		// Close the directory
		dir.close();
	}

	public static String getFileName(String indexName) {
		return indexName + FILENAME_POSTFIX;
	}

	/**
	 * Returns the schema of the B-tree directory records.
	 * 
	 * @param keyTypes
	 *            the types of the keys, which are indexed field in the
	 *            corresponding table
	 * 
	 * @return the schema of the index records
	 */
	static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		// XXX: I'am not sure if using "key_0" as the field name of the key
		// is a good design
		for (int i = 0; i < keyType.getNumOfFields(); i++)
			sch.addField(BTreeKeyReader.getKeyFldName(i), keyType.get(i));
		sch.addField(SCH_CHILD, BIGINT);
		return sch;
	}

	private static long getLevelFlag(BTreePage p) {
		return p.getFlag(0);
	}

	private static void setLevelFlag(BTreePage p, long val) {
		p.setFlag(0, val);
	}

	private static long getChildBlockNumber(BTreePage p, int slot) {
		return (Long) p.getVal(slot, SCH_CHILD).asJavaVal();
	}

	private static SearchKey getKey(BTreePage page, int slot, int numOfFlds) {
		return new SearchKey(new BTreeKeyReader(page, slot, numOfFlds), numOfFlds);
	}

	private SearchKeyType keyType;
	private Schema schema;
	private Transaction tx;
	private ConcurrencyMgr ccMgr;
	private BTreePage currentPage;
	private int numOfFlds;

	private List<BlockId> dirsMayBeUpdated;

	/**
	 * Creates an object to hold the contents of the specified B-tree block.
	 * 
	 * @param blk
	 *            a block ID refers to the specified B-tree block
	 * @param ti
	 *            the metadata of the B-tree directory file
	 * @param tx
	 *            the calling transaction
	 */
	BTreeDir(BlockId blk, SearchKeyType keyType, Transaction tx) {
		this.keyType = keyType;
		this.numOfFlds = keyType.getNumOfFields();
		this.tx = tx;
		this.schema = schema(keyType);
		ccMgr = tx.concurrencyMgr();
		currentPage = new BTreePage(blk, NUM_FLAGS, schema, tx);
	}

	/**
	 * Closes the directory page.
	 */
	public void close() {
		currentPage.close();
		dirsMayBeUpdated = null;
	}

	/**
	 * Returns the block number of the B-tree leaf block that contains the
	 * specified search key.
	 * 
	 * @param searchKey
	 *            the search key
	 * 
	 * @param leafFileName
	 *            the file name of the B-tree leaf file
	 * @return the BlockId of the leaf block containing that search key
	 */
	public BlockId search(SearchKey searchKey, String leafFileName, SearchPurpose purpose) {
		switch (purpose) {
		case READ:
			return searchForRead(searchKey, leafFileName);
		case INSERT:
			return searchForInsert(searchKey, leafFileName);
		case DELETE:
			return searchForDelete(searchKey, leafFileName);
		}
		throw new UnsupportedOperationException();
	}

	public List<BlockId> dirsMayBeUpdated() {
		return dirsMayBeUpdated;
	}

	/**
	 * Creates a new root block for the B-tree. The new root will have two
	 * children: the old root, and the specified block. Since the root must
	 * always be in block 0 of the file, the contents of block 0 will get
	 * transferred to a new block (serving as the old root). <br />
	 * Note that if the current page is not the root page, this method will
	 * automatically switch the current page to the root page.
	 * 
	 * @param newEntry
	 *            the directory entry to be added as a child of the new root.
	 *            The key of this entry must be larger than all keys in the
	 *            original root page.
	 */
	public void makeNewRoot(DirEntry newEntry) {
		// check that the content is the root block
		if (currentPage.currentBlk().number() != 0) {
			currentPage.close();
			currentPage = new BTreePage(new BlockId(currentPage.currentBlk().fileName(), 0), NUM_FLAGS, schema, tx);
		}
		SearchKey firstKey = getKey(currentPage, 0, numOfFlds);
		long level = getLevelFlag(currentPage);
		// split() will create a new page and transfer the entries to the new
		// page
		long newBlkNum = currentPage.split(0, new long[] { level });
		DirEntry oldRootEntry = new DirEntry(firstKey, newBlkNum);
		insert(oldRootEntry);
		insert(newEntry);
		setLevelFlag(currentPage, level + 1);
	}

	/**
	 * Insert a new entry. If the current page is full after inserting a new
	 * entry, it will split the current page and return a new entry indicating
	 * the new page.
	 * 
	 * @param e
	 *            the new entry
	 * @return null, if the page is not full; the entry of the new directory
	 *         which stores the second half of entries from the current page, if
	 *         the page is full
	 */
	public DirEntry insert(DirEntry e) {
		int newslot = 1 + findSlotBefore(e.key());
		insert(newslot, e.key(), e.blockNumber());
		if (!currentPage.isFull())
			return null;

		// if this page is full, split the page
		// more precisely, create a new page and transfer the second half of
		// entries to the new page
		int cuttingPoint = currentPage.getNumRecords() / 2;
		SearchKey splitVal = getKey(currentPage, cuttingPoint, numOfFlds);
		long newBlkNum = currentPage.split(cuttingPoint, new long[] { getLevelFlag(currentPage) });
		return new DirEntry(splitVal, newBlkNum);
	}

	/**
	 * Get the number of records in this page.
	 * 
	 * @return the number of records
	 */
	public int getNumRecords() {
		return currentPage.getNumRecords();
	}

	private BlockId searchForInsert(SearchKey searchKey, String leafFileName) {
		try {
			// search from root to level 0 (the lowest directory block)
			dirsMayBeUpdated = new ArrayList<BlockId>();
			BlockId parentBlk = currentPage.currentBlk();
			ccMgr.crabDownDirBlockForModification(parentBlk);
			long childBlkNum = findChildBlockNumber(searchKey);
			BlockId childBlk;
			dirsMayBeUpdated.add(parentBlk);

			// if it's not the lowest directory block
			while (getLevelFlag(currentPage) > 0) {
				// read child block
				childBlk = new BlockId(currentPage.currentBlk().fileName(), childBlkNum);
				ccMgr.crabDownDirBlockForModification(childBlk);
				BTreePage child = new BTreePage(childBlk, NUM_FLAGS, schema, tx);

				// crabs back the parent if the child is not possible to split
				if (!child.isGettingFull()) {
					for (int i = dirsMayBeUpdated.size() - 1; i >= 0; i--)
						ccMgr.crabBackDirBlockForModification(dirsMayBeUpdated.get(i));
					dirsMayBeUpdated.clear();
				}
				dirsMayBeUpdated.add(childBlk);

				// move current block to child block
				currentPage.close();
				currentPage = child;
				childBlkNum = findChildBlockNumber(searchKey);
				parentBlk = currentPage.currentBlk();
			}

			// get leaf block id
			BlockId leafBlk = new BlockId(leafFileName, childBlkNum);
			ccMgr.modifyLeafBlock(leafBlk); // exclusive lock
			return leafBlk;
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}
	
	// Note that we do not modify directories during deletion so that
	// we do not need to acquire exclusive locks on directories. 
	private BlockId searchForDelete(SearchKey searchKey, String leafFileName) {
		try {
			// search from root to level 0 (the lowest directory block)
			BlockId parentBlk = currentPage.currentBlk();
			ccMgr.crabDownDirBlockForRead(parentBlk);
			long childBlkNum = findChildBlockNumber(searchKey);
			BlockId childBlk;

			// if it's not the lowest directory block
			while (getLevelFlag(currentPage) > 0) {
				// read child block
				childBlk = new BlockId(currentPage.currentBlk().fileName(), childBlkNum);
				ccMgr.crabDownDirBlockForRead(childBlk);
				BTreePage child = new BTreePage(childBlk, NUM_FLAGS, schema, tx);

				// release parent block
				ccMgr.crabBackDirBlockForRead(parentBlk);
				currentPage.close();

				// move current block to child block
				currentPage = child;
				childBlkNum = findChildBlockNumber(searchKey);
				parentBlk = currentPage.currentBlk();
			}

			// get leaf block id
			BlockId leafBlk = new BlockId(leafFileName, childBlkNum);
			ccMgr.modifyLeafBlock(leafBlk); // exclusive lock
			ccMgr.crabBackDirBlockForRead(currentPage.currentBlk());
			return leafBlk;
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private BlockId searchForRead(SearchKey searchKey, String leafFileName) {
		try {
			// search from root to level 0 (the lowest directory block)
			BlockId parentBlk = currentPage.currentBlk();
			ccMgr.crabDownDirBlockForRead(parentBlk);
			long childBlkNum = findChildBlockNumber(searchKey);
			BlockId childBlk;

			// if it's not the lowest directory block
			while (getLevelFlag(currentPage) > 0) {
				// read child block
				childBlk = new BlockId(currentPage.currentBlk().fileName(), childBlkNum);
				ccMgr.crabDownDirBlockForRead(childBlk);
				BTreePage child = new BTreePage(childBlk, NUM_FLAGS, schema, tx);

				// release parent block
				ccMgr.crabBackDirBlockForRead(parentBlk);
				currentPage.close();

				// move current block to child block
				currentPage = child;
				childBlkNum = findChildBlockNumber(searchKey);
				parentBlk = currentPage.currentBlk();
			}

			// get leaf block id
			BlockId leafBlk = new BlockId(leafFileName, childBlkNum);
			ccMgr.readLeafBlock(leafBlk); // shared lock
			ccMgr.crabBackDirBlockForRead(currentPage.currentBlk());
			return leafBlk;
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private long findChildBlockNumber(SearchKey searchKey) {
		int slot = findSlotBefore(searchKey);
		if (getKey(currentPage, slot + 1, numOfFlds).equals(searchKey))
			slot++;
		return getChildBlockNumber(currentPage, slot);
	}

	/**
	 * Calculates the slot right before the one having the specified search key.
	 * 
	 * @param searchKey
	 *            the search key
	 * @return the position before where the search key goes
	 */
	private int findSlotBefore(SearchKey searchKey) {
//		int slot = 0;
//		while (slot < contents.getNumRecords() &&
//				getKey(contents, slot).compareTo(searchKey) < 0)
//			slot++;
//		return slot - 1;
		
		// Optimization: Use binary search rather than sequential search
		int startSlot = 0, endSlot = currentPage.getNumRecords() - 1;
		int middleSlot = (startSlot + endSlot) / 2;

		if (endSlot >= 0) {
			while (middleSlot != startSlot) {
				if (getKey(currentPage, middleSlot, numOfFlds).compareTo(searchKey) < 0)
					startSlot = middleSlot;
				else
					endSlot = middleSlot;

				middleSlot = (startSlot + endSlot) / 2;
			}

			if (getKey(currentPage, endSlot, numOfFlds).compareTo(searchKey) < 0)
				return endSlot;
			else if (getKey(currentPage, startSlot, numOfFlds).compareTo(searchKey) < 0)
				return startSlot;
			else
				return startSlot - 1;
		} else
			return -1;
	}

	private void insert(int slot, SearchKey key, long blkNum) {
		// Insert an entry to the page
		tx.recoveryMgr().logIndexPageInsertion(currentPage.currentBlk().fileName(), false, keyType,
				currentPage.currentBlk().number(), slot);
		currentPage.insert(slot);

		// Set the contents
		for (int i = 0; i < numOfFlds; i++)
			currentPage.setVal(slot, BTreeKeyReader.getKeyFldName(i), key.get(i));
		currentPage.setVal(slot, SCH_CHILD, new BigIntConstant(blkNum));
	}
}
