#include "test_util.h"


RC TEST_RM_PRIVATE_1F(const string &tableName)
{
    // Functions tested
    // 1. Delete Tuples
    // 2. Scan Empty table
    // 3. Delete Table 
    cout << "***** In TEST_RM_PRIVATE_1F *****" << endl;

    RC rc;
    RID rid;
//    int numTuples = 100000;
    int numTuples = 10000;
    void *returnedData = malloc(300);
    vector<RID> rids;
    vector<string> attributes;

    attributes.push_back("tweetid");
    attributes.push_back("userid");
    attributes.push_back("sender_location");
    attributes.push_back("send_time");
    attributes.push_back("referred_topics");
    attributes.push_back("message_text");

    readRIDsFromDisk(rids, numTuples);

    for(int i = 0; i < numTuples; i++)
    {
	cout<< rids[i].pageNum <<" "<<rids[i].slotNum<<endl;
        rc = rm->deleteTuple(tableName, rids[i]);
        if(rc != success) {
            free(returnedData);
            cout << "***** RelationManager::deleteTuple() failed. TEST_RM_PRIVATE_1F Failed *****" << endl << endl;
            return -1;
        }

        rc = rm->readTuple(tableName, rids[i], returnedData);
        if(rc == success) {
            free(returnedData);
            cout << "***** RelationManager::readTuple() should fail at this point. TEST_RM_PRIVATE_1F Failed *****" << endl << endl;
            return -1;
        }

        if (i % 10000 == 0){
            cout << i << " / " << numTuples << "have been processed." << endl;
        }
    }
	cout << "All records have been processed." << endl;
	
    // Set up the iterator
    RM_ScanIterator rmsi3;
    rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi3);
    if(rc != success) {
        free(returnedData);
        cout << "***** RelationManager::scan() failed. TEST_RM_PRIVATE_1F Failed *****" << endl << endl;
        return -1;
    }

    if(rmsi3.getNextTuple(rid, returnedData) != RM_EOF)
    {
        cout << "***** RM_ScanIterator::getNextTuple() should fail at this point. TEST_RM_PRIVATE_1F Failed *****" << endl << endl;
        rmsi3.close();
        free(returnedData);
        return -1;
    }
    rmsi3.close();
    free(returnedData);

    // Delete a Table
    rc = rm->deleteTable(tableName);
    if(rc != success) {
        cout << "***** RelationManager::deleteTable() failed. TEST_RM_PRIVATE_1F Failed *****" << endl << endl;
        return -1;
    }

    cout << "***** TEST_RM_PRIVATE_1F passed *****" << endl << endl;
    return 0;
}

int main()
{
    RC rcmain = TEST_RM_PRIVATE_1F("tbl_private_1");
    return rcmain;
}
