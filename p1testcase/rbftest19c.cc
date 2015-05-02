#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 
#include <vector> 

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "../rbf/test_util.h"

using namespace std;

int RBFTest_19c(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Increase Record's size and Update it
    // 5. Test Update Records / Deletion on Updated Records
    // 6. Close Record-Based File
    // 7. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 19c *****" << endl;
   
    RC rc;
    string fileName = "test19c";

    // Create a file named "test19c"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      
//    RID rid; 
    int recordSize = 0;
    void *record = malloc(200);
    void *returnedData = malloc(200);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    vector<RID> rids;
    vector<int> pageChangeIndex;
    int page = 0;
    cout << "Insert Data:" << endl;
    // Insert multiple record 
    for( int i=0; i<500; i++){
        // Insert a record into a file
	memset( record,0,200);
	memset( returnedData,0,200);
	prepareRecord(recordDescriptor.size(), nullsIndicator, 6, "Peters", 24, 170.1, 5000, record, &recordSize);
	rbfm->printRecord(recordDescriptor, record);
	RID rid; 
	rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
	if( page != rid.pageNum ){
	    if( i-1 > 0 ) pageChangeIndex.push_back( i-1 );
	    page = rid.pageNum;
	}
	assert(rc == success && "Inserting a record should not fail.");
	rids.push_back( rid );
    }

    string name = "PetersUpdated";
    // Test update function
    for( int i=0; i<450; i++) {
	memset( record,0,200);
	memset( returnedData,0,200);
	prepareRecord(recordDescriptor.size(), nullsIndicator, 6, "Peters", 24+i, 170.1, 5000, record, &recordSize);
	// call update function
	rc = rbfm->updateRecord(fileHandle, recordDescriptor, record ,rids[i]);
	assert( rc == success && "Updating a record should not fail.");
	// test reading the updated record 
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[i], returnedData );
	assert( rc == success && "Reading a updated record should not fail.");

	rc = rbfm->printRecord(recordDescriptor,returnedData);
	if( memcmp(record, returnedData, recordSize) != 0 ){
	    cout << "compare udpated record failed\n";
	    cout << "[FAIL] Test Case 19c Failed!" << endl << endl;
	    return -1;
	}
/*
	// test delete on the updated record 
	rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rids[ pageChangeIndex[i]] );
	assert( rc == success && "Deleting a updated record should not fail.");

    	// test reading the deleted updated record 
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[ pageChangeIndex[i] ], returnedData );
	assert( rc != success && "Reading a deleted record should fail.");
*/

    }


    // Close the file "test19c"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

	rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "[PASS] Test Case 19c Passed!" << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();
    
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test19c");
       
    RC rcmain = RBFTest_19c(rbfm);

    remove("test19c");
    return rcmain;
}
