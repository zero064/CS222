#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include "../rbf/test_util.h"

using namespace std;

int RBFTest_17a(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Delete Record
    // 6. Read Deleted Record
    // 7. Close Record-Based File
    // 8. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 17a *****" << endl;
   
    RC rc;
    string fileName = "test17a";

    // Create a file named "test17a"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      
    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file
    prepareRecord(recordDescriptor.size(), nullsIndicator, 6, "Peters", 24, 170.1, 5000, record, &recordSize);
    cout << "Insert Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");
   
    RID rid2; 
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid2);
    assert(rc == success && "Inserting a record should not fail.");
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid2);
    assert(rc == success && "Inserting a record should not fail.");
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid2);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 17a Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
    
    // delete a record
    cout<< "delete a record " << endl;
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    assert( rc == success && "delete the record should not fail.");

    rid.slotNum = 1;
    cout<< "delete a record " << endl;
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    assert( rc == success && "delete the record should not fail.");

    // read the record again, it should fail
    cout<< "revisit the record " << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert( rc != success && "read deleted record should fail.");

    // read the record again, it should not fail
    cout<< "revisit the record " << endl;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid2, returnedData);
    assert( rc == success && "read deleted record should fail.");
    rbfm->printRecord(recordDescriptor,returnedData);

    cout<< "revisit the record by scan" << endl;
    RBFM_ScanIterator rmsi;
    vector<string> attributes;
    for(int i=0; i<recordDescriptor.size();i++){
         attributes.push_back( recordDescriptor[i].name );
    }
    rc = rbfm->scan(fileHandle,recordDescriptor,"",NO_OP,NULL,attributes,rmsi);
    while( rmsi.getNextRecord(rid, returnedData) != RBFM_EOF){
	printf("%d %d\n",rid.pageNum,rid.slotNum);
	rbfm->printRecord(recordDescriptor,returnedData);
    }

    
    // Close the file "test17a"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

	rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "[PASS] Test Case 17a Passed!" << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();
    
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test17a");
       
    RC rcmain = RBFTest_17a(rbfm);

    remove("test17a");
    return rcmain;
}
