#include "test_util.h"

RC RM_TEST_EXTRA_3(const string &tableName, const int nameLength, const string &name, const int age, const int height, const int salary)
{
	// Extra Test Case - Functions Tested:
    // 1. Insert tuple
    // 2. Read Attributes
    // 3. Drop Attributes **
    cout << endl << "***** In RM Extra Credit Test Case 3 *****" << endl;

    RID rid;
    int tupleSize = 0;
    void *tuple = malloc(300);
    void *returnedData = malloc(300);

    // Insert Tuple
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);

    // Insert three same record with 4 attributes
    rc = rm->insertTuple(tableName, tuple, rid);
    rc = rm->insertTuple(tableName, tuple, rid);
    rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Read Attribute
    rc = rm->readAttribute(tableName, rid, "Height", returnedData);
    assert(rc == success && "RelationManager::readAttribute() should not fail.");

    if(memcmp((char *)returnedData+nullAttributesIndicatorActualSize, (char *)tuple+14+nullAttributesIndicatorActualSize, 4) != 0)
    {
        cout << "RelationManager::readAttribute() failed." << endl;
        cout << "***** [FAIL] Extra Credit Test Case 3 Failed. *****"<<endl;
        free(returnedData);
        free(tuple);
        return -1;
    }
    else
    {
	system("clear");
	cout<<"Starting add attributes, press any key to continue\n";
	cin.get();
	// Add the attribute
	Attribute weight;
	weight.name = "Weight";
	weight.type = TypeInt;
	weight.length = (AttrLength)4;
	weight.position = 0;
	rc = rm->addAttribute(tableName, weight);
        assert(rc == success && "RelationManager::addAttribute() should not fail.");

        // Read Tuple and print the tuple
        rc = rm->readTuple(tableName, rid, returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");
	system("clear");
	attrs.push_back(weight);
        rc = rm->printTuple(attrs, returnedData);
        assert(rc == success && "RelationManager::printTuple() should not fail.");
	cin.get();

        // Drop the attribute
        rc = rm->dropAttribute(tableName, "Height");
        assert(rc == success && "RelationManager::dropAttribute() should not fail.");

        // Read Tuple and print the tuple
        rc = rm->readTuple(tableName, rid, returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");

        // Remove "Height" attribute from the Attribute vector
        attrs.erase(attrs.begin()+2);
        rc = rm->printTuple(attrs, returnedData);
        assert(rc == success && "RelationManager::printTuple() should not fail.");
    }

    vector<string> attrNames;
    for(int i=0; i<attrs.size(); i++){
	attrNames.push_back( attrs[i].name );
    }

    RM_ScanIterator rmsi;
    rc = rm->scan(tableName, "", NO_OP, NULL, attrNames , rmsi);
    int i =0;
    while( rmsi.getNextTuple(rid,returnedData) != RM_EOF ){
	i++;
        rc = rm->printTuple(attrs, returnedData);
        assert(rc == success && "RelationManager::printTuple() should not fail.");
    }
    assert( i == 3 && "should have one record");
    free(tuple);
    free(returnedData);

    cout << "***** [PASS] Extra Credit Test Case 3 passed *****" << endl;
    return success;
}

int main()
{
    string name1 = "Peters";
    string name2 = "Victors";

    remove("tbl_employee200");
    // Drop Attribute
    RC rcmain = createTable("tbl_employee200");
    rcmain = RM_TEST_EXTRA_3("tbl_employee200", 6, name1, 24, 170, 5000);

    return rcmain;
}
