#include "BTreeNode.h"

using namespace std;

//Initialize private variables
BTLeafNode::BTLeafNode()
{
	tupleCount = 0;
	//Set every value in buffer to 0. This makes it easier mplementing cases where no keys exist.
	memset(buffer, 0, PageFile::PAGE_SIZE);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	return tupleCount; 
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	if(tupleCount < MAX_LEAF_RECORDS)
	{
		RC rc;
		int eid;
		rc = locate(key, eid);
		if(rc == 0){
			int offset = (sizeof(RecordId) + sizeof(int))*eid;
			//Create a temporary buffer that stores all tuples that have to be shifted
			int shiftSize = (sizeof(RecordId) + sizeof(int))*(tupleCount - eid);
			char *tempBuffer = (char *)malloc(shiftSize * sizeof(char));
			memcpy(tempBuffer, buffer + offset, shiftSize);
			//Insert new tuple
			memcpy(buffer + offset, &rid, sizeof(RecordId));
			memcpy(buffer + offset + sizeof(RecordId), &key, sizeof(int));
			//Shift the tuples after insert
			memcpy(buffer + offset + sizeof(RecordId) + sizeof(int), tempBuffer, shiftSize);
			tupleCount++;
			free(tempBuffer);
			return 0;
		}
	}
	//Full node
	return -1; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{
	if(tupleCount == MAX_LEAF_RECORDS){
		//The sibling node will have the same number of records in case of odd MAX_LEAF_RECORDS, less if even
		for(int eid=MAX_LEAF_RECORDS/2; eid < tupleCount; eid++){
			int key;
			RecordId rid;
			readEntry(eid, key, rid);
			sibling.insert(key, rid);
			//Set siblingKey as first key value
			if(eid == MAX_LEAF_RECORDS/2){
				siblingKey = key;
			}
		}
		
		//Delete tuples from the original node and save current next node's pid
		PageId currentNextPid = getNextNodePtr();
		int offset = (MAX_LEAF_RECORDS/2)*(sizeof(RecordId) + sizeof(int));
		memset(buffer + offset, 0, PageFile::PAGE_SIZE-offset);
		
		//Set new key count and next node ptr
		tupleCount = MAX_LEAF_RECORDS/2 + 1;
		sibling.setNextNodePtr(currentNextPid);
		
		//How to set pageid of next node to point to sibling though?
		
		return 0;
	}
	return -1; 
}

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	for(eid=0; eid<MAX_LEAF_RECORDS; eid++){
		int key;
		int offset = (sizeof(int) + sizeof(RecordId))*eid;
		memcpy(&key, buffer + offset + sizeof(RecordId), sizeof(int));
		if(key >= searchKey)
			return 0;
	}
	//All keys are smaller than searchKey
	eid = -1;
	return -1;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	//eid starts at 0, 27 is max value
	if(eid < tupleCount){
		int offset = (sizeof(int) + sizeof(RecordId))*eid;
		memcpy(&rid, buffer + offset, sizeof(RecordId));
		memcpy(&key, buffer + offset + sizeof(RecordId), sizeof(int));
		return 0;
	}
	return -1;//or a specific RC 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	PageId pid;
	memcpy(&pid, buffer+PageFile::PAGE_SIZE-sizeof(PageId), sizeof(PageId));
	return pid; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	if(pid >= 0){
		PageId pid;
		memcpy(buffer+PageFile::PAGE_SIZE-sizeof(PageId), &pid, sizeof(PageId));
		return 0; 
	}
	return -1; //probably return a specific RC later on
}

BTNonLeafNode::BTNonLeafNode()
{
	memset(buffer, 0, PageFile::PAGE_SIZE);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	return tupleCount; 
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
