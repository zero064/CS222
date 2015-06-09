#include "test_util.h"


int TEST_RM_PRIVATE_1D(const string &tableName)
{
    // Functions tested
    // 1. Scan Table (VarChar)
    cout << "***** In TEST_RM_PRIVATE_1D *****" << endl;

    RID rid; 
    vector<string> attributes;   
    void *returnedData = malloc(300);

    void *value = malloc(16);
    string msg = "Msg00999";
    int msgLength = 8;
    
    memcpy((char *)value, &msgLength, 4);
    memcpy((char *)value + 4, msg.c_str(), msgLength);
    
    string attr = "message_text";   
    attributes.push_back("sender_location");
    attributes.push_back("send_time");

    RM_ScanIterator rmsi2;
    RC rc = rm->scan(tableName, attr, LT_OP, value, attributes, rmsi2);
    if(rc != success) {
        free(returnedData);
        cout << "***** TEST_RM_PRIVATE_1D Failed *****" << endl << endl;
        return -1;
    }
    
    float sender_location = 0.0;
    float send_time = 0.0;
    
    int counter = 0;
    while(rmsi2.getNextTuple(rid, returnedData) != RM_EOF)
    {
        counter++;
    
    	sender_location = *(float *)((char *)returnedData + 1);
    	send_time = *(float *)((char *)returnedData + 5);
    	    
        if (!(sender_location >= 0.0 || sender_location <= 998.0 || send_time >= 2000.0 || send_time <= 2998.0))
        {
             cout << "***** A wrong Entry was returned. TEST_RM_PRIVATE_1D Failed *****" << endl << endl;
             rmsi2.close();
             free(returnedData);
             free(value);
             return -1;
        }
    }

    rmsi2.close();
    free(returnedData);
    free(value);
    
    if (counter != 999){
       cout << "***** The number of returned tuple: " << counter << " is not correct. TEST_RM_PRIVATE_1D failed *****" << endl << endl;
    } else {
       cout << "***** TEST_RM_PRIVATE_1D Passed *****" << endl << endl; 
    }
    return 0;
}

int main()
{
    RC rcmain = TEST_RM_PRIVATE_1D("tbl_private_1");
    return rcmain;
}
