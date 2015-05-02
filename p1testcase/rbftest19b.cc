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

int RBFTest_19b(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Increase Record's size and Update it
    // 5. Test Update Records / Deletion on Updated Records
    // 6. Close Record-Based File
    // 7. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 19b *****" << endl;
   
    RC rc;
    string fileName = "test19b";

    // Create a file named "test19b"
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
    cout << "Insert Data:" << endl;
    // Insert multiple record 
    for( int i=0; i<500; i++){
        // Insert a record into a file
	memset( record,0,200);
	memset( returnedData,0,200);
	prepareRecord(recordDescriptor.size(), nullsIndicator, 6, "Peters", i%70, 160.1+0.1*i, 5000, record, &recordSize);
	RID rid; 
	rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
	assert(rc == success && "Inserting a record should not fail.");
	rids.push_back( rid );
    }

    // Construct a iterator 
    RBFM_ScanIterator rmsi;

    // create test attributes
    string attr = "Height", attr2 = "Age";
    float heightVal = 185; int ageVal = 49;
    vector<string> attributes;
    for(int i=0; i<recordDescriptor.size();i++){
	attributes.push_back( recordDescriptor[i].name );
    }

    rc = rbfm->scan(fileHandle,recordDescriptor,attr,GT_OP,&heightVal,attributes,rmsi);
    assert( rc == success && "initalize scan should not fail");
    RID rid;
    int i=0;
    while( rmsi.getNextRecord(rid, returnedData) != RBFM_EOF){
	i++;
//	rbfm->printRecord(recordDescriptor, returnedData);
    }

    // (185 - 160.1) /0.1  ~= 250, should have 250 records that's greater than 185
    printf("total %d records\n",i);
    assert( i == 250 && "Number of records where height > 185 should be 250" );
 
    rc = rbfm->scan(fileHandle,recordDescriptor,attr2,GT_OP,&ageVal,attributes,rmsi);
    assert( rc == success && "initalize scan should not fail");
    vector<RID> deleteRID;
    i=0;
    while( rmsi.getNextRecord(rid, returnedData) != RBFM_EOF){
	i++;
	deleteRID.push_back(rid);
//	rbfm->printRecord(recordDescriptor, returnedData);
    }
    // age > 49 
    printf("total %d records\n",i);
    assert( i == 140 && "Number of records where age > 49 should be 140" );

    printf("delete %d records\n",deleteRID.size());
    for(int i=0; i<deleteRID.size();i++){
	rc = rbfm->deleteRecord(fileHandle,recordDescriptor,rid);
	assert( rc == success && "delete in scan should not fail");
    }
 
    // test NO_OP
    rc = rbfm->scan(fileHandle,recordDescriptor,attr2,NO_OP,&ageVal,attributes,rmsi);
    assert( rc == success && "initalize scan should not fail");

    i=0;
    while( rmsi.getNextRecord(rid, returnedData) != RBFM_EOF){
	i++;
//	rbfm->printRecord(recordDescriptor, returnedData);
    }
    printf("total %d records\n",i);
    assert( i == 500 && "Should be exact 500");

  

    // Close the file "test19b"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

	rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "[PASS] Test Case 19b Passed!" << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();
    
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test19b");
       
    RC rcmain = RBFTest_19b(rbfm);

    remove("test19b");
    return rcmain;
}
