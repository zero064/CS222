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

int RBFTest_17b(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Delete Record
    // 6. Read Deleted Record
    // 7. Close Record-Based File
    // 8. Destroy Record-Based File
    vector<RID> rids;
    vector<int> sizes;    
    cout << endl << "***** In RBF Test Case 17b *****" << endl;
   
    RC rc;
    string fileName = "test17b";

    // Create a file named "test17b"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      

//    int recordSize = 0;
    void *record = malloc(1000);
    int numRecords = 2000;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attr Name: " << recordDescriptor[i].name << " Attr Type: " << (AttrType)recordDescriptor[i].type << " Attr Len: " << recordDescriptor[i].length << endl;
    }
	cout << endl;
	
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert 2000 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

	RID rid; 
        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting the file should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);        
    }


    // delete multi record
    cout<< "deleting multi record " << endl;
    
    for( int i = 0; i< numRecords; i++){
	if( i % 50 ){
	    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rids[i]);
	    assert( rc == success && "delete the record should not fail.");


	    // read the record again, it should fail
	    void *returnedData = malloc(1000);
//	    cout<< "revisit the record " << endl;
	    rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[i], returnedData);
	    free(returnedData);
	    assert( rc != success && "read deleted record should fail.");

	}
    }

    // Close the file "test17b"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);

    cout << "[PASS] Test Case 17b Passed!" << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();
    
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test17b");
       
    RC rcmain = RBFTest_17b(rbfm);

//    remove("test17b");
    return rcmain;
}
