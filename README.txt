Name: Justin Wei
ID: 003942248
Email: Hottfro@gmail.com

Name: Nathan Lai
ID: 903787969
Email: nathan.lai@sbcglobal.net

Part A:
	We partnered up after Part A, we decided to use Justin's code for part A. We could have used either person's but just decided to choose one.
	
Part B: 
	We split the implementation of BTreeNode.cc in half. Justin was in charge of implementing the BTLeafNode and Nathan was in charge
	of implementing BTNonLeafNode. We both helped each other in debugging all the functions. For this part of the lab we decided to 
	make the maximum number of records for both types of nodes to be 60. It is possible to have a greater number of records in the nodes
	but we made the size big enough to fulfill the test requirements. One limitation on this size is that it needs to be able to hold
	an extra record for insertAndSplit function because of overflow. We also added in a private member variable called tupleCount to 
	count the number of tuples in each node, and also two helper functions getBufferPointer and changeKeyCount for insertAndSplit function.
	The rest of the code is as defined by the skeleton code given to us and our implementations should match the specifications given to us
	and behave similarly to the B+Tree diagram given to us.
