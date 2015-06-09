#include "test_util.h"

RC TEST_RM_PRIVATE_3(const string &tableName, const string &tableName1)
{
    // Functions tested
    // An attempt to modify System Catalogs tables - should fail
    cout << "**** In TEST_RM_PRIVATE_3 ****" << endl;

    RID rid; 
    vector<Attribute> attrs;
    void *returnedData = malloc(200);

    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttribute() should not fail.");

    // print attribute name
    cout << "Tables table: (";
    for(unsigned i = 0; i < attrs.size(); i++)
    {
        if (i < attrs.size() - 1) cout << attrs[i].name << ", ";
        else cout << attrs[i].name << ")" << endl << endl;
    }

	// Try to delete the system catalog
	rc = rm->deleteTable(tableName);
	if (rc == 0){
		cout << "***** [FAIL] The system catalog should not be deleted by a user call. TEST_RM_PRIVATE_3 Failed *****" << endl;
		free(returnedData);
		return -1;
	}

    // Set up the iterator
    RM_ScanIterator rmsi;
    vector<string> projected_attrs;
    if (attrs[1].name == "table-name") {
       projected_attrs.push_back(attrs[1].name);
    } else {
		cout << "***** [FAIL] The system catalog implementation is not correct. TEST_RM_PRIVATE_3 Failed *****" << endl;
		free(returnedData);
		return -1;
    }

    rc = rm->scan(tableName, "", NO_OP, NULL, projected_attrs, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

	int counter = 0;
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
		rc = rm->deleteTuple(tableName, rid);
		if (rc == 0) {
			cout << "***** [FAIL] The system catalog should not be modified by a user call. TEST_RM_PRIVATE_3 Failed *****" << endl;
		    rmsi.close();
		    rc = rm->deleteTable(tableName1);
		    free(returnedData);
			return -1;
		}
		counter++;
    }
    rmsi.close();
    
	// At least two system catalog tables and the currently created tables exist (Tables and Columns)
	if (counter < 3) {
		cout << "***** [FAIL] The system catalog implementation is not correct. TEST_RM_PRIVATE_3 Failed *****" << endl;
		return -1;
	}

	// Delete the table that is created in this test.
    rc = rm->deleteTable(tableName1);
    assert(rc == success && "RelationManager::deleteTable() should not fail.");
	
    cout << "***** TEST_RM_PRIVATE_3 finished. The result will be examined. *****" << endl;
    free(returnedData);
    return 0;
}

int main()
{
	createTable("tbl_private_3");
    RC rcmain = TEST_RM_PRIVATE_3("Tables", "tbl_private_3");
    return rcmain;
}
