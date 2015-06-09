#include "test_util.h"

RC TEST_RM_PRIVATE_1B(const string &tableName)
{
    // Functions tested
    // 1. Insert 100000 Tuples - NULLS included
    // 2. Read Attribute
    cout << endl << "***** In TEST_RM_PRIVATE_1B *****" << endl;

    RID rid;
    int tupleSize = 0;
    int numTuples = 100000;
    void *tuple;
    void *returnedData = malloc(300);

    vector<RID> rids;
    vector<char *> tuples;
    RC rc = 0;
    
    // GetAttributes
    vector<Attribute> attrs;
    rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    unsigned char *nullsIndicatorWithNulls = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);
	memset(nullsIndicatorWithNulls, 0, nullAttributesIndicatorActualSize);
	nullsIndicatorWithNulls[0] = 20; // 00010100 - the 4th and 6th column is null. - send_time and message_text
	
    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(300);

        // Insert Tuple
        float sender_location = (float)i;
	    float send_time = (float)i + 2000;
        int tweetid = i;
        int userid = i;
        stringstream ss;
        ss << setw(5) << setfill('0') << i;
        string msg = "Msg" + ss.str();
        string referred_topics = "Rto" + ss.str();

		if (i % 20 == 0) {
        	prepareTweetTuple(attrs.size(), nullsIndicatorWithNulls, tweetid, userid, sender_location, send_time, referred_topics.size(), referred_topics, msg.size(), msg, tuple, &tupleSize);
		} else {
        	prepareTweetTuple(attrs.size(), nullsIndicator, tweetid, userid, sender_location, send_time, referred_topics.size(), referred_topics, msg.size(), msg, tuple, &tupleSize);
		}        

        rc = rm->insertTuple(tableName, tuple, rid);
    	assert(rc == success && "RelationManager::insertTuple() should not fail.");

        tuples.push_back((char *)tuple);
        rids.push_back(rid);

        if (i % 5000 == 0){
           cout << (i+1) << "/" << numTuples << " records have been inserted so far." << endl;
        }
    }
	cout << "All records have been inserted." << endl;
	
	bool testFail = false;
	bool nullBit = false;
    for(int i = 0; i < numTuples; i++)
    {
        int attrID = rand() % 6;
        
        // Force attrID to be the ID that contains NULL when a i%20 is 0.
        if (i%20 == 0) {
        	attrID = 5;
        }
        string attributeName;
        if (attrID == 0) {
	    attributeName = "tweetid";
        } else if (attrID == 1) {
	    attributeName = "userid";
        } else if (attrID == 2) {
            attributeName = "sender_location";
        } else if (attrID == 3) {
            attributeName = "send_time";
        } else if (attrID == 4){
            attributeName = "referred_topics";
        } else if (attrID == 5){
            attributeName = "message_text";
        }
        rc = rm->readAttribute(tableName, rids[i], attributeName, returnedData);
    	assert(rc == success && "RelationManager::readAttribute() should not fail.");

		// NULL indicator should say that a NULL is returned.
		if (i%20 == 0) {

				nullBit = *(unsigned char *)((char *)returnedData) & (1 << 7);
				if (!nullBit) {
					cout << "A returned value from a readAttribute() is not correct." << endl;
					testFail = true;					
				}		
		} else {
			// tweetid
			if (attrID == 0) {
				if (memcmp(((char *)returnedData + 1), ((char *)tuples.at(i) + 1), 4) != 0) {
					testFail = true;
				}
			// userid
			} else if (attrID == 1) {
				if (memcmp(((char *)returnedData + 1), ((char *)tuples.at(i) + 5), 4) != 0) {
					testFail = true;
				}
			// sender_location
			} else if (attrID == 2) {
				if (memcmp(((char *)returnedData + 1), ((char *)tuples.at(i) + 9), 4) != 0) {
					testFail = true;
				}
			// send_time
			} else if (attrID == 3) {
				if (memcmp(((char *)returnedData + 1), ((char *)tuples.at(i) + 13), 4) != 0) {
					testFail = true;
				}
			// referred_topics
			} else if (attrID == 4) {
				if (memcmp(((char *)returnedData + 5), ((char *)tuples.at(i) + 21), 8) != 0) {
					testFail = true;
				}
			// message_text
			} else if (attrID == 5) {
				if (memcmp(((char *)returnedData + 5), ((char *)tuples.at(i) + 33), 8) != 0) {
					testFail = true;
				}
			}
		}
        
        if (testFail) {
		    cout << "***** TEST_RM_PRIVATE_1B failed ***** " << attrID <<  " " << i << endl << endl;
		    free(returnedData);
			for(int j = 0; j < numTuples; j++)
			{
				free(tuples[j]);
			}
		    rc = rm->deleteTable(tableName);
		    
		    return -1;
        }

    }

    free(returnedData);
    for(int i = 0; i < numTuples; i++)
    {
        free(tuples[i]);
    }
    
    cout << "***** TEST_RM_PRIVATE_1B passed *****" << endl << endl;

    return 0;
}

int main()
{
    createTweetTable("tbl_private_1b");
    RC rcmain = TEST_RM_PRIVATE_1B("tbl_private_1b");
    return rcmain;
}
