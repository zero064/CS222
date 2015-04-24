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

int RBFTest_18(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Read Each Attribute from Record
    // 6. Close Record-Based File
    // 7. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 18 *****" << endl;
   
    RC rc;
    string fileName = "test18";

    // Create a file named "test18"
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
   
    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");
 
    cout << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    cout << "\nRead each attribute separately" << endl;
    for( int i=0; i<recordDescriptor.size(); i++){
	void* field = malloc(60);
	rbfm->readAttribute( fileHandle, recordDescriptor, rid, recordDescriptor[i].name, field);
	vector<Attribute> tmp;
	tmp.push_back(recordDescriptor[i]);
	cout << "Attribute no. "<<i<<endl;
	rbfm->printRecord(tmp,field);
	free(field);
    }
    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 18 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
   
    // Close the file "test18"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

	rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "[PASS] Test Case 18 Passed!" << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();
    
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test18");
       
    RC rcmain = RBFTest_18(rbfm);

    remove("test18");
    return rcmain;
}
