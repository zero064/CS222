
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    // if file doesn't exist
    if ( access( fileName.c_str(), F_OK ) == -1  ){
	dprintf("file doen't exsit \n");
	FILE* file = fopen(fileName.c_str(),"w+b");
	if( file != NULL )
	   fclose(file);
	else 
	   return FAILURE;

	return SUCCESS;
    }

    return FAILURE;
}

RC IndexManager::destroyFile(const string &fileName)
{
    if( remove( fileName.c_str() ) != 0 )
	return FAILURE;
    else
	return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
    // file does exist, open it 
    if ( access( fileName.c_str(), F_OK ) == 0  ){
	dprintf("file does exist, open it \n");
	if( ixFileHandle.initFilePointer( fileName ) == FAILURE )
	    return FAILURE; 
	
	ixFileHandle.readPageCount = 0;
	ixFileHandle.writePageCount = 0;
	ixFileHandle.appendPageCount = 0;

	return SUCCESS;
    }

    return FAILURE;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return ixfileHandle.closeFilePointer();
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    RC rc;
    // find root first 
    PageNum root = ixfileHandle.findRootPage();
    void *page = malloc(PAGE_SIZE);
    rc = ixfileHandle.readPage(root,page); 
    
    // check if root needs to be split 
    NodeDesc nodeDesc;
    memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
    
    NodeType type = nodeDesc.type;
    PageSize size = nodeDesc.size;

    if( size > THRESHOLD )
	splitLeaf(ixfileHandle,attribute,key,rid,page);	    
    
    switch( type ){
	case Leaf:
	    
	    break;
	case NonLeaf:

	    break;
	default:
	    assert(false && "type fault");
	    break;
    }

    // traverse tree

    // 
    
    return -1;
}

RC IndexManager::splitLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page)
{
    
    
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
}


IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::initFilePointer(const string &fileName)
{
    return fileHandle.initFilePointer( fileName );
}

RC IXFileHandle::closeFilePointer()
{
    return fileHandle.closeFilePointer();
}

PageNum IXFileHandle::findFreePage()
{
    // PageNum ( unsigned int ) is 4 byte
    // A directory can have (4096 / 4) - 1 = 1023 cells to describe pages ( leaf & non-leaf );
    // Each directory start with index 1, where use 0 or 1 to indicate empty or used
    // The first directory is a specical case where 1st entry stores root node's pagenum ( positive integer )
    void *page = malloc(PAGE_SIZE);
    
    PageNum dirPageNum = 0 ; // Read first directory
   
    while( true ){
	// search from directory, if read directroy fail, means need to allocate new directory in file
	if( readPage(dirPageNum,page) == FAILURE ) {
	    for(int i=1; i< IXDirectorySize ; i++ ){
		unsigned int empty = 0;
		memcpy( (char*)page+i*sizeof(PageNum) , &empty, sizeof(PageNum) );
	    } 
	}
	
	// lookup free page in directory
	for(int i=1; i<IXDirectorySize; i++){
	    PageNum pageNum;
	    memcpy( &pageNum, (char*)page+sizeof(PageNum)*i , sizeof(PageNum) );
	    // find fresh page, update it to directory and return the page ID 
	    if( pageNum == 0 ){
		pageNum = 1;
		memcpy( (char*)page+i*sizeof(PageNum) , &pageNum, sizeof(PageNum) );
		writePage(dirPageNum,page);
		free(page);
		return (PageNum)(i+dirPageNum);
	    }
	}
	dirPageNum += IXDirectorySize; // find next directory;
    } 

    // un-reachable code
    free(page);
    assert(false && "should be unreachable");
}

RC IXFileHandle::updateRootPage(PageNum pageNum)
{
    void *page = malloc(PAGE_SIZE);
    // read root directory
    if( readPage(0, page) == FAILURE ) {
	assert( false && "Read root director should not fail" );
	return FAILURE;
    }
    memcpy( (char*)page+sizeof(PageNum), &pageNum, sizeof(PageNum) );
    RC rc;
    rc = writePage(0,page);
    assert( rc == SUCCESS && "something wrong in udpate rootpage");
    free(page);
    return SUCCESS;
}

PageNum IXFileHandle::findRootPage()
{
    // PageNum ( unsigned int ) is 4 byte
    // A directory can have (4096 / 4) - 1 = 1023 cells to describe pages ( leaf & non-leaf );
    // Each directory start with index 1, where use 0 or 1 to indicate empty or full
    // The first directory is a specical case where 1st entry stores root node's pagenum ( positive integer )

    int directorySize = PAGE_SIZE / sizeof( PageNum );
    void *page = malloc(PAGE_SIZE);
    PageNum root = 1; // assume root page is 1 if the whole structure hasn't been initialized 
    if( readPage(0, page) == FAILURE ) {
	for(int i=1; i< directorySize; i++ ){
	    PageNum empty = 0;
	    memcpy( (char*)page+i*sizeof(PageNum) , &empty, sizeof(PageNum) );
	} 
	memcpy( (char*)page+sizeof(PageNum), &root, sizeof(PageNum) ); // write root to 1st directory
	RC rc;
	rc = writePage(0, page);
	assert( rc == SUCCESS && "write root page failed" );

    }else{
	// read 1st directory's 1st entry
	memcpy( &root , (char*)page+sizeof(PageNum) , sizeof(PageNum) );
    }

    assert( root > 0 && "Root can not be page zero" );
    free(page);
    return root;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    return readPage(pageNum,data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    return fileHandle.writePage(pageNum,data);
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCount;
    writePageCount = this->writePageCount;
    appendPageCount = this->appendPageCount;
    return SUCCESS;
}

void IX_PrintError (RC rc)
{
}
