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
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");
  /*
  RecordId rid;
  rid.pid = 3;
  rid.sid = 5;
  BTreeIndex tree;
  tree.open("test.tbl", 'w');
  tree.insert(10, rid);
  tree.insert(8, rid);
  tree.insert(11, rid);
  tree.insert(3, rid);
  tree.insert(4, rid);
  tree.insert(9, rid);
  tree.insert(12, rid);
  tree.insert(13, rid);
  tree.insert(14, rid);
  tree.insert(15, rid);
  tree.insert(16, rid);
  tree.insert(17, rid);
  tree.insert(1, rid);
  tree.insert(2, rid);
  tree.insert(5, rid);
  tree.insert(6, rid);
  */
  
  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

bool conditionRange(const vector<SelCond>& cond, int &low, int &high)
{
	//Set low as lowest key value and highest key value possible
	low = 1;
	high = 2147483647;
	for(int i = 0; i < cond.size(); i++){
		int num = atoi(cond[i].value);
		if(cond[i].attr == 1){
			switch(cond[i].comp){
				case SelCond::EQ: 
					if(low > num || high < num)
						return false;
					low = num;
					high = num;
					break;
				case SelCond::NE:
					break;
				case SelCond::LT:
					if(low > num)
						return false;
					if(high > num-1)
						high = num-1;
					break;
				case SelCond::GT:
					if(high < num)
						return false;
					if(low < num+1)
						low = num+1;
					break;
				case SelCond::LE:
					if(low > num)
						return false;
					if(high > num)
						high = num;
					break;
				case SelCond::GE:
					if(high < num)
						return false;
					if(low < num)
						low = num;
					break;
			}
			//If two conditions contradict then return false
			if(low > high)
				return false;
		}
	}
	return true;
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
	
  BTreeIndex tree;
  bool index = false;
	
	//Open the table file
	if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}
	
	//Only need index if have a comparison on key, not including notequal
	for(int i = 0; i < cond.size(); i++){
		if(cond[i].attr == 1 && cond[i].comp != SelCond::NE){
			index = true;
			break;
		}
	}
	
	//Don't need tree if index file doesnt exist
  if(index && (rc = tree.open(table + ".idx", 'r')) < 0) {
		index = false;
  }
	
	//Start index and count in the beginning
	rid.pid = rid.sid = 0;
	count = 0;	
	
  if(index){
		int low, high;
		IndexCursor cursor;
		bool status;
		if(conditionRange(cond, low, high)){
			//Traverse values in the given range and print them out in the B+Tree
			if((rc = tree.locate(low, cursor)) < 0)
				goto exit_tree_select;
				
			while((rc = tree.readForward(cursor, key, rid)) >= 0 && key <= high){		
				// read the tuple
				if ((rc = rf.read(rid, key, value)) < 0) {
					fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
					goto exit_tree_select;
				}
				for(int i = 0; i < cond.size(); i++){
					switch(cond[i].comp){
						case 1:
							diff = key - atoi(cond[i].value);
							break;
						case 2:
							diff = strcmp(value.c_str(), cond[i].value);
							break;
					}

					// skip the tuple if any condition is not met
					switch (cond[i].comp){
						case SelCond::EQ:
							if (diff != 0) goto next_tree_tuple;
							break;
						case SelCond::NE:
							if (diff == 0) goto next_tree_tuple;
							break;
						case SelCond::GT:
							if (diff <= 0) goto next_tree_tuple;
							break;
						case SelCond::LT:
							if (diff >= 0) goto next_tree_tuple;
							break;
						case SelCond::GE:
							if (diff < 0) goto next_tree_tuple;
							break;
						case SelCond::LE:
							if (diff > 0) goto next_tree_tuple;
							break;
					}
					
					// the condition is met for the tuple. 
					// increase matching tuple counter
					count++;

					// print the tuple 
					switch (attr){
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
				}
				//Skip to next tuple
				next_tree_tuple:
				continue;
			}
			
			//Error checking, Ignore end of tree error
			if(rc < 0 && rc != -1015)
				goto exit_tree_select;
			rc = 0;
			
			// print matching tuple count if "select count(*)"
			if(attr == 4)
				fprintf(stdout, "%d\n", count);
				
		}else{
			//Condition conflict, select shouldnt print out anything
			rc = RC_CONDITION_CONFLICT;
			goto exit_tree_select;
		}

		exit_tree_select:
		tree.close();
		return rc;
  }else{
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
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
	RecordFile rf;
	RecordId rid;
	RC rc;
	BTreeIndex tree;
	ifstream file(loadfile.c_str());
	
	//open the table file and loadfile and BTreeIndex
	
	if(!file.is_open()){
		fprintf(stderr, "Error: Could not open file %s\n", loadfile.c_str());
		return -1;
	}
	
	if ((rc = rf.open(table + ".tbl", 'w')) < 0){
		fprintf(stderr, "Error: Error creating or writing to table %s\n", table.c_str());
		return rc;
	}
	
	if(index){
		if ((rc = tree.open(table + ".idx",'w')) < 0){
			fprintf(stderr, "Error: Error creating or writing to tblname.idx");
			return rc;
		}
	}
	
	//Read in tuples
	while(!file.eof()){
		int key;
		string line, value;
		getline(file, line);
		
		if((rc = parseLoadLine(line, key, value)) < 0){
			rf.close();
			file.close();
			if(index)
				tree.close();
		}
		
		//Ignore empty lines
		if(key != 0 || strcmp(value.c_str(), "") != 0){
			//Write to table
			rf.append(key, value, rid);
			//Write to tree
			if(index){
				if((rc = tree.insert(key, rid)) < 0){
					rf.close();
					file.close();
					tree.close();
					return rc;
				}
			}			
		}
	}
	rf.close();
	file.close();
	if(index)
		tree.close();	
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
