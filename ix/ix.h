#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <cstdio>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
const int IXDirectorySize = PAGE_SIZE / sizeof(PageNum) ; // 1024
typedef unsigned short int PageSize;

typedef char NodeType;
const char Leaf = 1 , NonLeaf = 2;

const int maxvarchar = PAGE_SIZE;
const int THRESHOLD = PAGE_SIZE / 2;
const PageNum InvalidPage = 0;

typedef enum { OP_Split, OP_Merge , OP_Dist , OP_None , OP_Error }TreeOp; 

typedef struct {
	NodeType type;
	PageSize size;
	PageNum next = InvalidPage;
	PageNum prev = InvalidPage;
}NodeDesc;

const int UpperThreshold = (PAGE_SIZE-sizeof(NodeDesc))*0.9;
const int LowerThreshold = (PAGE_SIZE-sizeof(NodeDesc))*0.5;




typedef struct {
	PageNum leftNode;
	PageNum rightNode;
	short int keySize;
	void *keyValue;
} KeyDesc;

typedef struct {
	PageNum overflow = InvalidPage;
	short int numOfRID;
	short int keySize;
	void *keyValue;
} DataEntryDesc;

const int DataEntryKeyOffset =  sizeof(DataEntryDesc) ;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager : public DebugMsg {
	friend class IX_ScanIterator;
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
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page, int depth, PageNum nodeNum);

	protected:
	IndexManager();
	~IndexManager();

	private:
	static IndexManager *_index_manager;
	void FindLastKey(void *page,KeyDesc &keyDesc);

	void checkKeyInt(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page);

	void checkPageInt(IXFileHandle &ixfileHandle, void *page, PageNum pageNum,bool p);


	RC FindOffset(void *page,int size,int &offset,bool IsGreater);

	TreeOp TraverseTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page, PageNum pageNum, PageNum &returnpageNum);

	TreeOp TraverseTreeDelete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc,int rightMost);

	TreeOp TraverseTreeInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc);


	TreeOp insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc);


	TreeOp deleteFromLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page,

			PageNum pageNum, KeyDesc &keyDesc, int rightMost);


	int keyCompare(const Attribute &attribute, const void *keyA, const void* keyB); 
	int getKeySize(const Attribute &attribute, const void *key);
	void printKey(const Attribute &attribute, const void *key);
	bool unsync; // deletion sync flag
	/*
	   splitLeaf()
	   splitNonLeaf()
	   bool checkSplit( void *page );

	   RC TraverseTree(PageNum, const void *key, const RID &rid);

	   RC makeKey(	
	   RC readKey(void *page, int offset, KeyDesc &keyDesc);
	   RC releaseKey
	   */
};


class IXFileHandle : public DebugMsg {
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
		RC deletePage(PageNum pageNum);

		bool root_debug = false;
		PageNum findFreePage();
		PageNum findRootPage();
		RC updateRootPage(PageNum pageNum);
		RC isReadable();
	private:
		FileHandle fileHandle;
		RC error;

};




class IX_ScanIterator : public DebugMsg{

	public:
		IX_ScanIterator();  							// Constructor
		~IX_ScanIterator(); 							// Destructor

		RC getNextEntry(RID &rid, void *key);			 		// Get next matching entry
		RC close();             						// Terminate index scan

		// Initialize and IX_ScanIterator to supports a range search
		RC init(IXFileHandle &ixfileHandle,
				const Attribute &attribute,
				const void *lowKey,
				const void *highKey,
				bool lowKeyInclusive,
				bool highKeyInclusive);

	private:
		IXFileHandle ixfileHandle;
		IndexManager *im;
		Attribute attribute;
		void *lowKey; bool lowKeyNull , highKeyNull;
		void *highKey;
		bool lowKeyInclusive, highKeyInclusive;
		void *page, *overflowPage;
		bool overflow;
		int offsetToKey, offsetToRID, overflowRID;
		PageNum currentOverflowPage, pageNum;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
