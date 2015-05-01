#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <stdlib.h>
#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
    unsigned pageNum;	// page number
    unsigned slotNum; // slot number in the page
} RID;
// Record offset in page
typedef struct 
{
    short int offset;
    short int length;
}   RecordOffset;

typedef struct
{
    short int numOfSlot;  // record number of slot in the page
			  // if the value is negative, the slot list is discontinous
			  // need to perform a linear search to find empty slot
    short int recordSize; // total record size
}   PageDesc;

const short int DeletedSlotMark = -1; 
const short int TombStoneMark = -1; 

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;
typedef unsigned AttrPosition;

typedef short int FieldSize;
typedef unsigned short int FieldOffset;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
    AttrPosition position; // attribute position
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { 
	NO_OP = 0,  // no condition
	EQ_OP,      // =
	LT_OP,      // <
        GT_OP,      // >
        LE_OP,      // <=
        GE_OP,      // >=
        NE_OP,      // !=
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();
class RecordBasedFileManager;

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() { page = malloc(PAGE_SIZE); };
  ~RBFM_ScanIterator() { free(page); };

  RC initScanIterator(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames); // a list of projected attributes
  // "data" follows the same format as RecordBasedFileManager::insertRecord()
  RC getNextRecord(RID &rid, void *data); //{ return RBFM_EOF; };
  RC close(); // { return -1; };

private:
     RecordBasedFileManager *rbfm;
     FileHandle fileHandle;
     vector<Attribute> recordDescriptor;
     string conditionAttribute;
     CompOp compOp;
     void *value, *page;
     vector<string> attributeNames;
     unsigned pageNum = 1; 
     RID c_rid; PageDesc pageDesc;
     RC getFormattedRecord(void *returnedData, void *data);
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field contains null value or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If thre are more than 8 fields, then you need to find the corresponding byte, then a bit inside that byte.
  //  2) actual data is a concatenation of values of the attributes
  //  3) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/**************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the rid does not change after update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private: 
  static RecordBasedFileManager *_rbf_manager;
  PagedFileManager *pagedFileManager;
  


// custom private variable
  

// custom private function
  PageNum findFreePage(FileHandle &fileHandle,int recordSize);
  RC updateFreePage(FileHandle &fileHandle,int deletedSize,int pageNum);
  size_t getDataSize(const vector<Attribute> &recordDescriptor, const void *data, bool printFlag);  
  size_t writeDataToBuffer(const vector<Attribute> &recordDescriptor, const void *data, void * &formattedData);
  RC readDataFromBuffer(const vector<Attribute> &recordDescriptor, void *data, const void * formattedData);
  size_t getRecordFromPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void * &returnedData);
};

#endif
