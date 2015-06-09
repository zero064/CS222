#include "test_util.h"


RC TEST_RM_PRIVATE_1C(const string &tableName)
{
    // Functions tested
    // 1. Scan Table 
    cout << "***** In TEST_RM_PRIVATE_1C *****" << endl;

    RID rid;    
    int numTuples = 100000;
    void *returnedData = malloc(300);
    set<int> user_ids;
    readUserIdsFromDisk(user_ids, numTuples);     

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "userid";
    vector<string> attributes;
    attributes.push_back(attr);
    RC rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    if(rc != success) {
        cout << "***** TEST_RM_PRIVATE_1C failed *****" << endl << endl;
        return -1;
    }
    
    int userid = 0;
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
    	userid = *(int *)((char *)returnedData + 1);

        if (user_ids.find(userid) == user_ids.end())
        {
            cout << "***** TEST_RM_PRIVATE_1C Failed *****" << endl << endl;
            rmsi.close();
            free(returnedData);
            return -1;
        }
    }
    rmsi.close();
    free(returnedData);

    cout << "***** TEST_RM_PRIVATE_1C Passed *****" << endl << endl; 
    return 0;
}

int main()
{
    RC rcmain = TEST_RM_PRIVATE_1C("tbl_private_1");
    return rcmain;
}
