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
    return pf.open(indexname, mode);
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
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
	//Tree is empty
	if(rootPid == -1 || treeHeight == 0){
		char info[PageFile::PAGE_SIZE];
		rootPid = pf.endPid();
		BTLeafNode leafNode;
		leafNode.insert(key, rid);
		leafNode.write(rootPid, pf);
		leafNode.print();
		treeHeight++;
		
		IndexCursor cursor;
		cursor.pid = -1;
		cursor.eid = -1;
		printf("error: %d\n", locate(10, cursor));
		printf("pid:%d, eid:%d\n", cursor.pid, cursor.eid);
	}

	return -1;
	RC errorCode;
	
	//find the correct lead node/position that fits the key and try to insert rid
	//traverseToLeafNode (key, LeafNode);
	//errorCode = LeafNode.read(key, currentNode);
	if (errorCode != 0)
		return errorCode;
	//errorCode = LeafNode.insert(key, rid);
	//if node is filled, split the nodes and the nodes above it as many times as needed
	if (errorCode == RC_NODE_FULL){}
	//overflow process
	return errorCode;
	
	return -1;
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
		return -1;
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
    if(cursor.pid < 0)
        return RC_INVALID_PID;

	if(cursor.eid < 0)
		return RC_INVALID_ATTRIBUTE;
		
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
