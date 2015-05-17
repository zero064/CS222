#include "test_util.h"

RC TEST_RM_8(const string &tableName, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested for large tables:
    // 1. getAttributes
    // 2. insert tuple
    cout << endl << "***** In RM Test Case *****" << endl;

    RID rid; 
    void *tuple = malloc(2000);
    int numTuples = 50000;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    printf("%d\n",attrs.size());
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

//    for(unsigned i = 0; i < attrs.size(); i++)
//    {
//        cout << "Attr Name: " << attrs[i].name << "  Type: " << (AttrType) attrs[i].type << " Len: " << attrs[i].length << endl;
//    }
    int numNull = 0;
    // Insert 2000 tuples into table
    for(int i = 0; i < numTuples; i++)
    {
        // Test insert Tuple
        int size = 0;
        memset(tuple, 0, 2000);
	if( i % 2000 == 0 ){
	    numNull++;
	    prepareLargeTupleNull(attrs.size(), nullsIndicator, i, tuple, &size);
	    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);
	}else{
	    prepareLargeTuple(attrs.size(), nullsIndicator, i, tuple, &size);
	}

        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success && "RelationManager::insertTuple() should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);        
    }

    free(tuple);
    writeRIDsToDisk(rids);
    writeSizesToDisk(sizes);


    vector<string> attrS;
    for(int i=0; i<attrs.size(); i++){
	attrS.push_back( attrs[i].name );
    }
    int counter = 0;
    RM_ScanIterator rmsi;
    void *returnedData = malloc(2000);
    char v = 1;
    void *value = malloc(sizeof(char));
    memcpy( value, &v, 1);

    rc = rm->scan(tableName, "attr0", NE_OP, value, attrS, rmsi); 
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF){
	rm->deleteTuple(tableName,rid);
	counter ++;
    }
    cout<<counter<<" "<<numNull<<endl;
    assert( counter == numTuples && "error ");
    
    cout << "***** [PASS] Test Case Passed *****" << endl << endl;
    return success;
}

int main()
{
    vector<RID> rids;
    vector<int> sizes;
    //rm->deleteTable("tbl_employeetest");
    rm->deleteCatalog();
    rm->createCatalog();
    createLargeTable("tbl_employeetest");
	// Insert Tuple
    RC rcmain = TEST_RM_8("tbl_employeetest", rids, sizes);

    return rcmain;
}
