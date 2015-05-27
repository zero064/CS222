#include <iostream>
#include <cstdio>
#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int assertScanVailid(IXFileHandle &ixfileHandle, const Attribute &attribute, 
        IX_ScanIterator & ix_ScanIterator, const int inRecordNum){
    // Scan
    assertInitalizeScan(success, indexManager, ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);

    int outRecordNum = 0;
    RID rid;
    unsigned key;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {

        if (rid.pageNum != key +1 ||  rid.slotNum != key + 2){
            cerr << "Wrong entries output...failure" << endl;
            ix_ScanIterator.close();
            return fail;
        }
        outRecordNum += 1;
    }
cout<<"Hi";
    if (inRecordNum != outRecordNum)
    {
        cerr << "Wrong entries output...failure" << endl;
        ix_ScanIterator.close();
        return fail;
    }
    return success;
}

int testCase_LargeDataSet(const string &indexFileName, const Attribute &attribute){
    // Create Index file
    // Open Index file
    // Insert large number of records
    // Scan large number of records to validate insert correctly
    // Delete  some
    // Insert large number of records again
    // Scan large number of records to validate insert correctly
    // Delete all
    // Close Index
    // Destry Index

    cerr << endl << "****Test Iteration Deletion ****" << endl;

    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned key;
    int inRecordNum = 0;
   // unsigned numOfTuples = 1000 * 100;
    unsigned numOfTuples = 1*100 * 10;

    // create index file
    assertCreateIndexFile(success, indexManager, indexFileName);

    // open index file
    assertOpenIndexFile(success, indexManager, indexFileName, ixfileHandle);

    // insert entry
    for(unsigned i = 0; i <= numOfTuples; i++)
    {
        key = i; 
	for( int j = 0; j < 300; j++ ){ 
	    rid.pageNum = key+1+j;
	    rid.slotNum = key+2+j;
	    assertInsertEntry(success, indexManager, ixfileHandle, attribute, &key, rid);
	    inRecordNum += 1;
	}
    }

    
    assertInitalizeScan(success, indexManager, ixfileHandle, attribute, 
            NULL, NULL, false ,false, ix_ScanIterator);
    int count = 0;
    // DeleteEntry in IndexScan Iterator
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if (rid.pageNum % 100 == 0) {
            cerr << "returned rid: " << rid.pageNum << " " << rid.slotNum << endl;
        }
        assertDeleteEntry(success, indexManager, ixfileHandle, attribute, &key, rid);
	count ++;
    }
    cerr << endl;
    cerr << "delete count "<< count << " numoftuples"<<inRecordNum<<endl;
    assert(count == inRecordNum && "deletion within scan failed");

    // Close Scan
    assertCloseIterator(success, ix_ScanIterator);

    // Close Index
    assertCloseIndexFile(success, indexManager, ixfileHandle);

    assertDestroyIndexFile(success, indexManager, indexFileName);
    return success;
}

int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

    const string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    RC result = testCase_LargeDataSet(indexFileName, attrAge);
    if (result == success) {
        cerr << "Iteration Deletion passed" << endl;
        return success;
    } else {
        cerr << "Iteration Deletion failed" << endl;
        return fail;
    }

}

