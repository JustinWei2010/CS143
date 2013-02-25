#include "BTreeNode.h"
#include <iostream> //testing

using namespace std;

//Initialize private variables
BTLeafNode::BTLeafNode()
{
	tupleCount = 0;
	//Set every value in buffer to 0. This makes it easier mplementing cases where no keys exist.
	memset(buffer, 0, PageFile::PAGE_SIZE);
}

//Testing
void BTLeafNode::print()
{
	printf("numKeys: %d\n", tupleCount);
	for(int eid = 0; eid < tupleCount; eid++){
		int key;
		RecordId rid;
		readEntry(eid, key, rid);
		printf("key: %d, page: %d, record: %d\n", key, rid.pid, rid.sid);
	}
	printf("next: %d\n", getNextNodePtr());
	return;
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
	if(tupleCount < MAX_LEAF_RECORDS){
		RC rc;
		int eid;
		rc = locate(key, eid);
		if(rc == 0){
			int offset = (sizeof(RecordId) + sizeof(int))*eid;
			//Create a temporary buffer that stores all tuples that have to be shifted
			int shiftSize = (keyRecordComponentSize)*(tupleCount - eid);
			char *tempBuffer = (char *)malloc(shiftSize * sizeof(char));
			memcpy(tempBuffer, buffer + offset, shiftSize);
			//Insert new tuple
			memcpy(buffer + offset, &rid, sizeof(RecordId));
			memcpy(buffer + offset + sizeof(RecordId), &key, sizeof(int));
			//Shift the tuples after insert
			memcpy(buffer + offset + keyRecordComponentSize, tempBuffer, shiftSize);
			tupleCount++;
			free(tempBuffer);
		return 0;
		}
	}
	//Full node
	return RC_NODE_FULL;
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
	//Make sure node is full
	if(tupleCount < MAX_LEAF_RECORDS){ 
        return -1;
    }
	
	//Get the entry id where the tuple should be entered 
	int eid = 0;
	for(; eid<tupleCount; eid++){
		int curKey;
		RecordId rid;
		readEntry(eid, curKey, rid);
		if(curKey >= key)
			break;
	}
	
	int start = MAX_LEAF_RECORDS/2;
	//Split will be uneven unless start is changed in this case, entry inserted into sibling and MAX_LEAF_RECORDS is odd
	if(eid > MAX_LEAF_RECORDS/2 && MAX_LEAF_RECORDS % 2 == 1)
		start = MAX_LEAF_RECORDS/2 + 1;
	
	
	//Insert entries to sibling
	for(int sid=start; sid < MAX_LEAF_RECORDS; sid++){
		int key;
		RecordId rid;
		readEntry(sid, key, rid);
		sibling.insert(key, rid);
		//Set siblingKey as first key value
		if(sid == MAX_LEAF_RECORDS/2){
			siblingKey = key;
		}
	}

	
	//Delete tuples from the original node
	PageId currentNextPid = getNextNodePtr();
	int offset = (start)*(sizeof(RecordId) + sizeof(int));
	memset(buffer + offset, 0, PageFile::PAGE_SIZE-offset);
	
	//Set new key count and next node ptr
	tupleCount = start;
	sibling.setNextNodePtr(currentNextPid);

	//Insert new node depending on where entry id is at
	if(eid <= MAX_LEAF_RECORDS/2)
		insert(key, rid);
	else
		sibling.insert(key,rid);

	//Set pageid of next node outside of this function
	return 0;
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
		int offset = (keyRecordComponentSize)*eid;
		memcpy(&key, buffer + offset + sizeof(RecordId), sizeof(int));
		//assume that no keys added can be 0
		if(key >= searchKey || key == 0)
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
	
	if(eid < 0 || eid >= tupleCount){
		return RC_INVALID_ATTRIBUTE;
	}

	int offset = (keyRecordComponentSize)*eid;
	memcpy(&rid, buffer + offset, sizeof(RecordId));
	memcpy(&key, buffer + offset + sizeof(RecordId), sizeof(int));
	return 0;
	
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
		memcpy(buffer+PageFile::PAGE_SIZE-sizeof(PageId), &pid, sizeof(PageId));
		return 0;
	}
	return RC_INVALID_PID;
}

BTNonLeafNode::BTNonLeafNode()
{
	tupleCount = 0;
	memset(buffer, 0, PageFile::PAGE_SIZE);
	
	//implement last node pointer
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
* Change the counter stating the number of keys stored in a node.
* @update tupleCount
*/
void BTNonLeafNode::changeKeyCount(const int& newKeyCount)
{
	tupleCount = newKeyCount;
}

/*
* Return the pointer to the node's buffer.
* @return the pointer to the node's buffer
*/
char* BTNonLeafNode::getBufferPointer()
{
	return &(buffer[0]);
}

void BTNonLeafNode::print()
{
	printf("numKeys: %d\n", tupleCount);
	for(int eid = 0; eid < tupleCount; eid++){
		int key;
		PageId pid;
		int offset = (keyPageComponentSize)*eid;
		memcpy(&pid, buffer + offset, sizeof(PageId));
		memcpy(&key, buffer + offset + sizeof(PageId), sizeof(int));
		printf("page: %d, key: %d\n", pid, key);
	}
	PageId pid;
	memcpy(&pid, buffer + keyPageComponentSize * tupleCount, sizeof(PageId)); 
	printf("last: %d\n", pid);
	return;
}

/*
* Insert a (key, pid) pair to the node.
* @param key[IN] the key to insert
* @param pid[IN] the PageId to insert
* @return 0 if successful. Return an error code if the node is full.
*/
RC BTNonLeafNode::insert(int key, PageId pid)
{		
	if(pid < 0){
		return RC_INVALID_PID;
	}

	if(tupleCount >= MAX_LEAF_RECORDS){
		return RC_NODE_FULL;
	}
	
	int eid = 0;
	for(; eid<tupleCount; eid++){
		int curKey;
		int offset = (keyPageComponentSize)*eid;
		memcpy(&curKey, buffer + offset + sizeof(PageId), sizeof(int));
		//Skip first pid, add key then pid after unlike leaf node
		if(curKey >= key){
			int shiftSize = (keyPageComponentSize)*(tupleCount - eid);
			
			//Shift tuples
			memmove(buffer + keyPageComponentSize*(eid+1) + sizeof(PageId), buffer + (keyPageComponentSize)*eid + sizeof(PageId), shiftSize);
			
			//Insert new tuple
			memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId), &key, sizeof(int));
			memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId) + sizeof(int), &pid, sizeof(PageId));
			
			tupleCount++;
			return 0;
		}
	}
	
	//Key is greater than all other keys in the node. Add it in at the end.
	//Will never have a key that is smaller than everything else because of the way we initialize and insert
	memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId), &key, sizeof(int));
	memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId) + sizeof(int), &pid, sizeof(PageId));
	tupleCount++;
	return 0;
}

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
{
	int numberOfCopiedTuples = (MAX_LEAF_RECORDS)/2;
	
	//make sure sibling node is empty (since constructor makes all values 0, we should be able to safely check if all values are 0
	char* siblingBuffer = sibling.getBufferPointer();
	for (int i = 0; i < PageFile::PAGE_SIZE ; i++){
		if (siblingBuffer[i] != 0)
			return -1;
	}
	
	//Make sure node is full
	if(tupleCount < MAX_LEAF_RECORDS){ 
        return -1;
    }
	
	//Insert tuple into node, node will overflow, but buffer should have enough excess space to hold the overflow
	int eid = 0;
	for(; eid<MAX_LEAF_RECORDS; eid++){
		int curKey;
		int offset = (keyPageComponentSize)*eid;
		memcpy(&curKey, buffer + offset + sizeof(PageId), sizeof(int));
		//Skip first pid, add key then pid after unlike leaf node
		if(curKey >= key){
			int shiftSize = (keyPageComponentSize)*(tupleCount - eid);
			
			//Shift tuples
			memmove(buffer + keyPageComponentSize*(eid+1) + sizeof(PageId), buffer + (keyPageComponentSize)*eid + sizeof(PageId), shiftSize);
			
			//Insert new tuple
			memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId), &key, sizeof(int));
			memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId) + sizeof(int), &pid, sizeof(PageId));
			tupleCount++;
			break;
		}
	}
	
	//Key is greater than all other keys in the node. Add it in at the end.
	//Will never have a key that is smaller than everything else because of the way we initialize and insert	
	if(eid == MAX_LEAF_RECORDS){
		memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId), &key, sizeof(int));
		memcpy(buffer + keyPageComponentSize*(eid) + sizeof(PageId) + sizeof(int), &pid, sizeof(PageId));
		tupleCount++;
	}
	
	//move (smaller) half of the tuples into the sibling buffer and then make sure the original node is clean
	memmove(siblingBuffer, buffer + (keyPageComponentSize*(tupleCount - numberOfCopiedTuples)),(keyPageComponentSize*numberOfCopiedTuples) + sizeof(PageId));
	memset(buffer + (keyPageComponentSize*(tupleCount - numberOfCopiedTuples)),0, (keyPageComponentSize*numberOfCopiedTuples) + sizeof(PageId));
	
	//get midKey and remove that key
	memcpy(&midKey, buffer + (keyPageComponentSize*(MAX_LEAF_RECORDS - numberOfCopiedTuples)) + sizeof(PageId), sizeof(int));
	memset(buffer + (keyPageComponentSize*(MAX_LEAF_RECORDS - numberOfCopiedTuples)) + sizeof(PageId),0, sizeof(int));
	
	//update key count for both nodes
	changeKeyCount(MAX_LEAF_RECORDS - numberOfCopiedTuples);
	sibling.changeKeyCount(numberOfCopiedTuples);
		
	//Set midkey to parent node outside function	
		
	return 0;
}

/*
* Given the searchKey, find the child-node pointer to follow and
* output it in pid.
* @param searchKey[IN] the searchKey that is being looked up.
* @param pid[OUT] the pointer to the child node to follow.
* @return 0 if successful. Return an error code if there is an error.
*/
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	if(searchKey <= 0){
		return RC_INVALID_ATTRIBUTE;
	}
	
	for(int eid=0; eid<tupleCount; eid++){
		int key;
		memcpy(&key, buffer + (keyPageComponentSize*eid) + sizeof(PageId), sizeof(int));
		if(key >= searchKey){
			memcpy(&pid, buffer + (keyPageComponentSize*eid), sizeof(PageId));
			return 0;
		}
	}

	//if it is not smaller than any of the other nodes, return the last node
	memcpy(&pid, buffer + (keyPageComponentSize*tupleCount), sizeof(PageId));
	return 0;
}

/*
* Initialize the root node with (pid1, key, pid2).
* @param pid1[IN] the first PageId to insert
* @param key[IN] the key that should be inserted between the two PageIds
* @param pid2[IN] the PageId to insert behind the key
* @return 0 if successful. Return an error code if there is an error.
*/
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
    if(pid1<0 || pid2<0)
        return RC_INVALID_PID;
	
	memcpy(buffer, &pid1, sizeof(PageId));
	memcpy(buffer + sizeof(PageId), &key, sizeof(PageId));
	memcpy(buffer + keyPageComponentSize, &pid2, sizeof(PageId));
	
	//set tupleCount to 1
	tupleCount = 1;
	
	return 0;
}
