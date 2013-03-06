Name: Justin Wei
ID: 003942248
Email: Hottfro@gmail.com

Name: Nathan Lai
ID: 903787969
Email: nathan.lai@sbcglobal.net

Part A:
	We partnered up after Part A, we decided to use Justin's code for part A. We could have used either person's but just decided to choose one.
	
Part B: 
	We split the implementation of BTreeNode.cc in half. Justin was in charge of implementing the BTLeafNode and Nathan was in charge of implementing BTNonLeafNode. We both helped each other in debugging all the functions. For this part of the lab we decided to make the maximum number of records for both types of nodes to be 60. It is possible to have a greater number of records in the nodes but we made the size big enough to fulfill the test requirements. One limitation on this size is that it needs to be able to hold an extra record for insertAndSplit function because of overflow. We also added in a private member variable called tupleCount to count the number of tuples in each node, and also two helper functions getBufferPointer and changeKeyCount for insertAndSplit function. The rest of the code is as defined by the skeleton code given to us and our implementations should match the specifications given to us and behave similarly to the B+Tree diagram given to us.
	
Part C:
	We split the implementation of BTreeIndex.cc in a different way than BTreeNode.cc. Since, the bulk of the code in index is consisted in the insert function we split the work mostly amongst that particular function. The other functions we just both did some of each. For the insert function Justin was in charge of implementing insert to work with leaves and Nathan was in charge of implementing insert to work with nonleaves. We had to both brainstorm before we were able to find a way to implement the insert function correctly. We were also both in charge of debugging the code. We had to change Part B from last time so that tupleCount was stored on disk as we realized that it was the only way to get a tupleCount after a read. We also made a few bug fixes in B, but none of them are drastic. For part C we added in two helper functions called traverseToLeafNode and traverseAndInsert. We used traverseToLeafNode for locate, so that we could seperate the traversing from the main function. We used traverseAndInsert as a recursive function, so that we could traverse through the tree and make inserts at nodes where necessary due to overflow. We also found out that we had to store treeHeight and rootPid onto disk, so we decided to store them in the first page and each tree node on each successive page.
	
Part D: