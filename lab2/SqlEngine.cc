/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream> //for ostringstream
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h" //added for testing
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");
  /*
  BTNonLeafNode node;
  BTNonLeafNode sibling;
  int midKey;
  node.initializeRoot(0,4,5);
  node.insert(7, 0);
  node.insert(2, 0);
  node.insert(9, 0);
  node.insertAndSplit(10, 0, sibling, midKey);
  node.print();
  printf("midkey:%d\n", midKey);
  sibling.print();
  */
  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning

	RC     rc;
	int    key;     
	string value;
	int    count;
	int    diff;
	
	//variables for index
	BTreeIndex treeIndex;
	IndexCursor indxCursor;
	vector<int> keyVector;
	vector<string> valueVector;
	vector<int> notEqualVector;
	int startingKey = 0;
	int endingKey; //default is the ending value
	bool geTrigger = false;
	bool leTrigger = false;

	// open the table file
	if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}
	
	//check if there is an index
	rf.read(rf.endRid(), key, value);
	
	// scan the table file from the beginning
	rid.pid = rid.sid = 0;
	count = 0;
	
	//if there is an index
	if(key == 46339 && value.compare(0,5,"index")) {
		//define tree index's private functions and variables
		//incomplete
		//find out how to do value search (things are in alphabetical order, so how do you get the values)
		//you can probably only do key search, value search will be done by going through the valueVectors
		
		//create the bounds for which you search for the key
		for (unsigned i = 0; i < cond.size(); i++) {
			if(cond[i].attr == 1) {
			//for now, only deal with key search values. Value conditions will be dealt with later since they can't be optimized by the tree
				switch (cond[i].comp) {
					case SelCond::EQ:
						startingKey = atoi(cond.value);
						endingKey = atoi(cond.value);
						bool geTrigger = true;
						bool leTrigger = true;
						break;
					case SelCond::NE:
						notEqualVector.push_back(atoi(cond.value));
						break;
					case SelCond::GT:
						if (atoi(cond.value) > startingKey) {
							startingKey = atoi(cond.value);
							bool geTrigger = false;
						}
						break;
					case SelCond::LT:
						if (atoi(cond.value) < endingKey || endingKey == 1015) {
							endingKey = atoi(cond.value);
							bool leTrigger = false;
						}
						break;
					case SelCond::GE:
						if (atoi(cond.value) >= startingKey) {
							startingKey = atoi(cond.value);
							bool geTrigger = true;
						}
						break;
					case SelCond::LE:
						if (atoi(cond.value) <= endingKey || endingKey == 1015) {
							endingKey = atoi(cond.value);
							bool leTrigger = true;
						}
				}
			}
		}
		//read the cond to find out the key to search
		treeIndex.locate(startingKey, indxCursor);
		//get the key the starting key
		while (key < endingKey) {
			//find the value of the value variable for the key
			//handling if there was a >= sign
			if(key == startingKey && geTrigger){
				keyVector.push_back(key);
				valueVector.push_back(value);
			}
			else {
				keyVector.push_back(key);
				valueVector.push_back(value);
			}
			treeIndex.readForward(indxCursor, key, rid);
			count++; //do I really need this
			if (key == -1015)
				break;
		}
		//handling if there was a >= sign (also, if = sign, it would skip the version in the for loop)
		if(key == endingKey && leTrigger){
				//find the value of the value variable for the key
				keyVector.push_back(key);
				valueVector.push_back(value);
		}
		//handle cases for value, now that the table it needs to search through is as small as it can be
		for (unsigned i = 0; i < cond.size(); i++) {
			if(cond[i].attr == 2) {
				for(int j = 0; j < valueVector.size; j++){
					if (cond[i].value == valueVector[j]) {
						//remove teh value in both keyVector and valueVector
					}
				}
			}
		}
		//print the tuples
		switch (attr) {
			case 1:  // SELECT key
				for (int i=0; i < keyVector.size(); i++) {
					fprintf(stdout, "%d\n", keyVector[i]);
					break;
				}
			case 2:  // SELECT value
				for (int i=0; i < keyVector.size(); i++) {
					fprintf(stdout, "%s\n", valueVector[i].c_str());
					break;
				}
			case 3:  // SELECT *
				for (int i=0; i < keyVector.size(); i++) {
					fprintf(stdout, "%d '%s'\n", keyVector[i], valueVector[i].c_str());
					break;
				}
		}
	}
	
	else {
		// scan the table file from the beginning (w/o index)
		while (rid < rf.endRid()) {
			// read the tuple
			if ((rc = rf.read(rid, key, value)) < 0) {
				fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
				goto exit_select;
			}

			// check the conditions on the tuple
			for (unsigned i = 0; i < cond.size(); i++) {
				// compute the difference between the tuple value and the condition value
				switch (cond[i].attr) {
					case 1:
						diff = key - atoi(cond[i].value);
						break;
					case 2:
						diff = strcmp(value.c_str(), cond[i].value);
						break;
				}
				// skip the tuple if any condition is not met
				switch (cond[i].comp) {
					case SelCond::EQ:
						if (diff != 0) goto next_tuple;
						break;
					case SelCond::NE:
						if (diff == 0) goto next_tuple;
						break;
					case SelCond::GT:
						if (diff <= 0) goto next_tuple;
						break;
					case SelCond::LT:
						if (diff >= 0) goto next_tuple;
						break;
					case SelCond::GE:
						if (diff < 0) goto next_tuple;
						break;
					case SelCond::LE:
						if (diff > 0) goto next_tuple;
						break;
				}
			}

			// the condition is met for the tuple. 
			// increase matching tuple counter
			count++;

			// print the tuple 
			switch (attr) {
				case 1:  // SELECT key
					fprintf(stdout, "%d\n", key);
					break;
				case 2:  // SELECT value
					fprintf(stdout, "%s\n", value.c_str());
					break;
				case 3:  // SELECT *
					fprintf(stdout, "%d '%s'\n", key, value.c_str());
					break;
			}

			// move to the next tuple
			next_tuple:
			++rid;
		}
	}
	
	// print matching tuple count if "select count(*)"
	if (attr == 4) {
		fprintf(stdout, "%d\n", count);
	}
	rc = 0;

	// close the table file and return
	exit_select:
	rf.close();
	return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* implement index part in later parts of lab */
	BTreeIndex treeIndex;
	RecordFile rf;
	RecordId rid;
	ostringstream convert;
	RC rc = 0;
	ifstream file(loadfile.c_str());
	
	//open the table file, loadfile, and BTreeIndex
	if(!file.is_open()){
		fprintf(stderr, "Error: Could not open file %s\n", loadfile.c_str());
		return -1;
	}
	
	if ((rc = rf.open(table + ".tbl", 'w')) < 0) {
		fprintf(stderr, "Error: Error creating or writing to table %s\n", table.c_str());
		return rc;
	}
	
	if ((rc = treeIndex.open("tblname.idx",'w')) < 0) {
		fprintf(stderr, "Error: Error creating or writing to tblname.idx");
		return rc;
	}
	
	//read in file
	int key;
	string line, value;
	while(!file.eof()){
		getline(file, line);
		if((rc = parseLoadLine(line, key, value)) < 0){
			rf.close();
			file.close();
			return rc;
		}
		if(key != 0 || strcmp(value.c_str(), "") != 0)
			rf.append(key, value, rid);
		if (index) {
			if((rc = treeIndex.insert(key, rid)) < 0){
				rf.close();
				file.close();
				return rc;
			}
		}
	}
	//save treeIndex file constants before closing (Needs fixing)
	key = 46339; //index in telophone keypad numbers
	//adding on the tree information into the value
	convert << rid.pid;
	value = "index," + convert.str();
	convert << rid.sid;
	value += "," + convert.str();
	rf.append(key, value, rid);
	//close all of the files
	treeIndex.close();
	rf.close();
	file.close();
	return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
