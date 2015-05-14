#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdlib.h>
#include <unistd.h>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
const int IXDirectorySize = PAGE_SIZE / sizeof(PageNum) ; // 1024
typedef unsigned short int PageSize;

typedef char NodeType;
const char Leaf = 1 , NonLeaf = 2;

const int THRESHOLD = PAGE_SIZE / 2;

typedef struct {
    NodeType type;
    PageSize size;
} NodeDesc;

typedef struct {
    Attribute type;
    PageNum leftNode;
    PageNum rightNode;
    void *keyValue;
} KeyDesc;

typedef struct {
    KeyType type;
    short int numOfRID;
    short int size
    void *keyValue;
} DataEntryDesc;


class IX_ScanIterator;
class IXFileHandle;

class IndexManager : public DebugMsg {

    public:
        static IndexManager* instance();

        // Create an index file
        RC createFile(const string &fileName);

        // Delete an index file
        RC destroyFile(const string &fileName);

        // Open an index and return a file handle
        RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

        // Close a file handle for an index. 
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given fileHandle
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to supports a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree JSON record in pre-order
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;


	RC splitNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page);

	splitLeaf()
	splitNonLeaf()
	bool checkSplit( void *page );
/*
	RC TraverseTree(PageNum, const void *key, const RID &rid);

	RC makeKey(	
	RC readKey(void *page, int offset, KeyDesc &keyDesc);
	RC releaseKey
	int compareKey( 
*/
};

class IX_ScanIterator {
    public:
        IX_ScanIterator();  							// Constructor
        ~IX_ScanIterator(); 							// Destructor

        RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
        RC close();             						// Terminate index scan
};


class IXFileHandle {
    public:
        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        IXFileHandle();  							// Constructor
        ~IXFileHandle(); 							// Destructor
	
	unsigned readPageCount, writePageCount, appendPageCount;
        // Initialize the file pointer for an index.
	RC initFilePointer(const string &fileName);

        // Close a file handle for an index. 
        RC closeFilePointer();

	// PageNum from pfm is unsigned int
	RC readPage(PageNum pageNum, void *data);
	RC writePage(PageNum pageNum, const void *data);

	PageNum findFreePage();
	PageNum findRootPage();
	RC updateRootPage(PageNum pageNum);
		
    private:
	FileHandle fileHandle;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
