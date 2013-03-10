/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
	treeHeight = 0;
  rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC errorCode;
	if((errorCode = pf.open(indexname, mode)) < 0)
		return errorCode;
	
	//Set or retrieve treeHeight and rootPid from first page
	if(pf.endPid() <= 0){
		//Tree has not been initialized yet
		char buffer[PageFile::PAGE_SIZE];
		memset(buffer, 0, PageFile::PAGE_SIZE);
		memcpy(buffer, &treeHeight, sizeof(int));
		memcpy(buffer + sizeof(int), &rootPid, sizeof(PageId));
		if((errorCode = pf.write(0, buffer)) < 0)
			return errorCode;
	}else{
		char buffer[PageFile::PAGE_SIZE];
		if((errorCode = pf.read(0, buffer)) < 0)
			return errorCode;
		memcpy(&treeHeight, buffer, sizeof(int));
		memcpy(&rootPid, buffer + sizeof(int), sizeof(PageId));
	}
  return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	RC errorCode;
	char buffer[PageFile::PAGE_SIZE];
	memset(buffer, 0, PageFile::PAGE_SIZE);
	memcpy(buffer, &treeHeight, sizeof(int));
	memcpy(buffer + sizeof(int), &rootPid, sizeof(PageId));
	if((errorCode = pf.write(0, buffer)) < 0)
		return errorCode;
  return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	RC errorCode = 0;
	if(rootPid == -1 || treeHeight == 0){
		//Tree is empty
		rootPid = pf.endPid();
		//Increment if rootPid is on the page where treeHeight and rootPid are stored
		if(rootPid == 0)
			rootPid ++;
		BTLeafNode leafNode;
		if((errorCode = leafNode.insert(key, rid)) < 0)
			return errorCode;
		//Next node ptr should be undefined, end of tree
		if((errorCode = leafNode.setNextNodePtr(RC_END_OF_TREE)) < 0)
			return errorCode;
		if((errorCode = leafNode.write(rootPid, pf)) < 0)
			return errorCode;
		treeHeight++;
		return 0;
	}else{
		//Tree is not empty, so traverse it
		int sibKey = -1;
		PageId sibPid = -1;
		return traverseAndInsert(key, rid, rootPid, sibKey, sibPid, treeHeight);
	}
}

RC BTreeIndex::traverseAndInsert(int key, const RecordId rid, PageId pid, int &sibKey, PageId &sibPid, int level){
	RC errorCode;
	if(level != 1){
		//At a non-leaf level
		BTNonLeafNode nonLeafNode;
		if((errorCode = nonLeafNode.read(pid,pf)) < 0)
			return errorCode;
		PageId traversePid;
		if((errorCode = nonLeafNode.locateChildPtr(key, traversePid)) < 0)
			return errorCode;
		//Recursively insert
		if((errorCode = traverseAndInsert(key, rid, traversePid, sibKey, sibPid, level-1)) < 0)
			return errorCode;	
		
		if(sibKey != -1 && sibPid != -1){
			//Insertion to nonLeafNode
			if(nonLeafNode.getKeyCount() >= MAX_LEAF_RECORDS){
				//Nonleaf overflow
				BTNonLeafNode siblingNode;
				if((errorCode = nonLeafNode.insertAndSplit(sibKey, sibPid, siblingNode, sibKey)) < 0)
					return errorCode;
				sibPid = pf.endPid();
				if((errorCode = nonLeafNode.write(pid, pf)) < 0)
					return errorCode;	
				if((errorCode = siblingNode.write(sibPid, pf)) < 0)
					return errorCode;					
				//Need to initialize a new root
				if(pid == rootPid){
					rootPid = pf.endPid();
					BTNonLeafNode rootNode;
					if((errorCode = rootNode.initializeRoot(pid, sibKey, sibPid)) < 0)
						return errorCode;
					if((errorCode = rootNode.write(rootPid, pf)) < 0)
						return errorCode;
					treeHeight++;	
				}
				return 0;
			}else{
				//No overflow
				if((errorCode = nonLeafNode.insert(sibKey,sibPid)) < 0)
					return errorCode;
				if((errorCode = nonLeafNode.write(pid, pf)) < 0)
					return errorCode;
			
				//Only need to add in record after split on the nonleaf node right above the leaf split. 
				sibKey = -1;
				sibPid = -1;
				return 0;
			}
		}
	}else{
		//At the leaf level
		BTLeafNode leafNode;
		if((errorCode = leafNode.read(pid,pf)) < 0)
			return errorCode;
		
		//Insertion to leafNode
		if(leafNode.getKeyCount() >= MAX_LEAF_RECORDS){
			//Leaf node overflow
			BTLeafNode siblingNode;
			sibPid = pf.endPid();
			//Insert tuple and split
			if((errorCode = leafNode.insertAndSplit(key, rid, siblingNode, sibKey)) < 0)
				return errorCode;
			if((errorCode = leafNode.setNextNodePtr(sibPid)) < 0)
				return errorCode;
			if((errorCode = leafNode.write(pid, pf)) < 0)
				return errorCode;	
			if((errorCode = siblingNode.write(sibPid, pf)) < 0)
				return errorCode;
			//Need to initialize a new root
			if(pid == rootPid){				
				rootPid = pf.endPid();
				BTNonLeafNode rootNode;
				if((errorCode = rootNode.initializeRoot(pid, sibKey, sibPid)) < 0)
					return errorCode;
				if((errorCode = rootNode.write(rootPid, pf)) < 0)
					return errorCode;
				treeHeight++;
			}
			return 0;
		}else{
			//If leaf node is not full, simple case
			if((errorCode = leafNode.insert(key,rid)) < 0)
				return errorCode;
			if((errorCode = leafNode.write(pid, pf)) < 0)
				return errorCode;
			return 0;
		}
	}
}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	RC errorCode = 0;
	BTLeafNode leafNode;
	
	//Check for if tree is empty
	if(rootPid < 0 || treeHeight < 1){
		return RC_TREE_EMPTY;
	}
	
	//Traverse to leaf node
	if((errorCode = traverseToLeafNode (searchKey, cursor.pid)) < 0)
		return errorCode;

	//Read in the node and locate the entry number
	if((errorCode = leafNode.read(cursor.pid, pf)) < 0){
		return errorCode;
	}
	
	return leafNode.locate(searchKey, cursor.eid);
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	if(cursor.pid == RC_END_OF_TREE)
			return RC_END_OF_TREE;

    if(cursor.pid < 0)
        return RC_INVALID_PID;

	if(cursor.eid < 0)
		return RC_INVALID_EID;
		
	BTLeafNode leafNode;
	RC errorCode = 0;
	
	//get the record and key from the current location
	if((errorCode = leafNode.read(cursor.pid, pf)) < 0)
		return errorCode;		
	if((errorCode = leafNode.readEntry(cursor.eid, key, rid)) < 0)
		return errorCode;
	
	//update cursor with the new entree (add 1 to eid)
	cursor.eid++;
	//If at the end of a node, set cursor on next node
	if(cursor.eid >= leafNode.getKeyCount()){
		cursor.pid = leafNode.getNextNodePtr();
		cursor.eid = 0;
	}
	
	return errorCode;
}

RC BTreeIndex::traverseToLeafNode(int searchKey, PageId& leafPid)
{
	PageId currentPid = rootPid;
	BTNonLeafNode NonLeafNode;
	RC errorCode = 0;
	//define NonLeafNode as root to start
	//traverse down the tree
	for(int i = 1; i < treeHeight; i++){
		//for each tree level, find which node to follow
		if((errorCode = NonLeafNode.read(currentPid, pf)) < 0)
			return errorCode;
			
		if((errorCode = NonLeafNode.locateChildPtr(searchKey, currentPid)) < 0)
			return errorCode;
	}
	leafPid = currentPid;
	return errorCode;
}
