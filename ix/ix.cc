
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

TreeOp IndexManager::TraverseTreeInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc)
{
	void *bufferpage = malloc(PAGE_SIZE);
	void *nextpage = malloc(PAGE_SIZE);
	NodeDesc nodeDesc;
	NodeDesc tempnodeDesc;
	NodeDesc nextnodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));

	PageSize offset=0;
	KeyDesc siblingkeyDesc;
	KeyDesc currentkeyDesc;
	currentkeyDesc.keyValue = malloc(maxvarchar);
	KeyDesc nextkeyDesc;
	nextkeyDesc.keyValue = malloc(maxvarchar);
	PageNum currentpageNum=-1;

	TreeOp treeop = OP_None;
	TreeOp nexttreeop = OP_None;

	//scan to find the desired pointer
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		memcpy(currentkeyDesc.keyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;

		if(keyCompare(attribute,key,currentkeyDesc.keyValue)<0){
			//get the page pointer
			currentpageNum=currentkeyDesc.leftNode;
			offset -= sizeof(KeyDesc);//adjust the offset for inserting a  key entry,
			offset -= currentkeyDesc.keySize;
			break;
		}
		if(offset == nodeDesc.size){
			//last entry
			currentpageNum=currentkeyDesc.rightNode;
			break;
		}
	}
	assert( currentpageNum != -1 && "Should find a pageNum");

	if(nodeDesc.size >= UpperThreshold){
		//split the page
		int splitoffset=0;
		NodeDesc tempnodeDesc;
		PageSize origsize=nodeDesc.size;

		while(true){

			//use keyDesc
			memcpy(&keyDesc,(char *) page+splitoffset,sizeof(KeyDesc));
			splitoffset += sizeof(KeyDesc);
			splitoffset += keyDesc.keySize;


			if(splitoffset >= UpperThreshold/2){

				break;
			}
		}
		//create nodeDesc for two pages
		nodeDesc.size = splitoffset;
		nodeDesc.next = ixfileHandle.findFreePage();

		//push up a key value
		memcpy(&keyDesc,(char *) page+splitoffset,sizeof(KeyDesc));
		splitoffset += sizeof(KeyDesc);
		memcpy(keyDesc.keyValue,(char *) page+splitoffset,keyDesc.keySize);
		splitoffset += keyDesc.keySize;
		keyDesc.leftNode = pageNum;
		keyDesc.rightNode = nodeDesc.next;



		tempnodeDesc.size = origsize-splitoffset;
		tempnodeDesc.prev = pageNum;
		tempnodeDesc.type = NonLeaf;
		assert(splitoffset < origsize && "splitoffset should be less than origsize");

		memcpy(bufferpage,(char *)page+splitoffset,origsize-splitoffset);
		memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
		memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
		ixfileHandle.writePage(pageNum,page);
		ixfileHandle.writePage(nodeDesc.next,bufferpage);
		treeop = OP_Split;


	}


	//recursively call TraverseTreeInsert
	ixfileHandle.readPage(currentpageNum,nextpage);
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	if(nextnodeDesc.type == Leaf){
		nexttreeop = insertToLeaf(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, nextkeyDesc);

		assert((nexttreeop == OP_Split || nexttreeop == OP_None) && "nexttreeop should be OP_split or OP_None");

	}else if(nextnodeDesc.type == NonLeaf){

		nexttreeop = TraverseTreeInsert(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, nextkeyDesc);
		assert((nexttreeop == OP_Split || nexttreeop == OP_None) && "nexttreeop should be OP_split or OP_None");


	}else{
		assert("page type should be leaf or NonLeaf");
	}
	if(nexttreeop == OP_Split){
		if(treeop == OP_Split ){

			if(offset = nodeDesc.size){
				offset -= nodeDesc.size;

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)bufferpage+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)bufferpage+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//may cause problem
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,(char *)bufferpage+offset,tempnodeDesc.size-offset);
				memcpy((char *)bufferpage+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)bufferpage+offset+sizeof(keyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);

				tempnodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(nodeDesc.next,bufferpage);



			}else if(offset > nodeDesc.size){
				offset -= nodeDesc.size;
				offset -= sizeof(KeyDesc);
				offset -= keyDesc.keySize;

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)bufferpage+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)bufferpage+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//may cause problem
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,(char *)bufferpage+offset,tempnodeDesc.size-offset);
				memcpy((char *)bufferpage+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)bufferpage+offset+sizeof(keyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);

				tempnodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(nodeDesc.next,bufferpage);

			}else{

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));

				memcpy((char *)page+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)page+offset+sizeof(keyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);

				nodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(pageNum,page);

			}

		}else if(treeop == OP_None){

			//update sibling KeyDesc
			memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
			siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
			memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));

			memcpy((char *)page+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,(char *)page+offset,nodeDesc.size-offset);
			memcpy((char *)page+offset,&nextkeyDesc,sizeof(KeyDesc));
			memcpy((char *)page+offset+sizeof(keyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);

			nodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
			ixfileHandle.writePage(pageNum,page);

		}else{
			assert("treeop should be OP_split or OP_None");
		}
	}

	free(nextpage);
	free(bufferpage);
	free(currentkeyDesc.keyValue);
	free(nextkeyDesc.keyValue);
	return treeop;
}


RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    RC rc;
    // find root first 
    PageNum root = ixfileHandle.findRootPage();
    void *page = malloc(PAGE_SIZE);
    rc = ixfileHandle.readPage(root,page); 
    
    KeyDesc keyDesc;
    keyDesc.type=attribute;
    keyDesc.keyValue=malloc(maxvarchar);

    // check if root needs to be split 
    NodeDesc nodeDesc;
    memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
    
    NodeType type = nodeDesc.type;
    PageSize size = nodeDesc.size;

    // traverse tree
    if(type==Leaf){
    	//root page is leaf page

    	TreeOp treeop=insertToLeaf(ixfileHandle, attribute, key, rid, page, root, keyDesc);
    	assert( ((treeop == OP_Split) || (treeop == OP_None)) && "treeop should be OP_Split or OP_None"  );
    	if(treeop == OP_Split){
    		PageNum newroot;
    		newroot=ixfileHandle.findFreePage();
    		ixfileHandle.updateRootPage(newroot);
    		NodeDesc newnodeDesc;
    		int keysize = getKeySize(keyDesc.type,keyDesc.keyValue);

    		newnodeDesc.next=-1;
    		newnodeDesc.type=NonLeaf;
    		newnodeDesc.size=0;

    		//reuse void * page, copy keyDesc to the page
    		memcpy((char *)page+newnodeDesc.size,&keyDesc,sizeof(KeyDesc));
    		newnodeDesc.size+=sizeof(KeyDesc);
    		memcpy((char *)page+newnodeDesc.size,keyDesc.keyValue,keysize);
    		newnodeDesc.size+=keysize;

    		memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&newnodeDesc,sizeof(NodeDesc));

    		rc = ixfileHandle.writePage(newroot,page);
    		assert(rc == SUCCESS && "Fail to write root page as leaf page");
    		dprintf("Successfully split root page");
    	}

    	free(keyDesc.keyValue);
    	free(page);
    	dprintf("Original root page is Leaf");
    	return 0;
    }else if(type == NonLeaf){
    	//root page is NonLeaf

       	TreeOp treeop=TraverseTreeInsert(ixfileHandle, attribute, key, rid, page, root, keyDesc);
        	assert( ((treeop == OP_Split) || (treeop == OP_None)) && "treeop should be OP_Split or OP_None"  );
        	if(treeop == OP_Split){
        		PageNum newroot;
        		newroot=ixfileHandle.findFreePage();
        		ixfileHandle.updateRootPage(newroot);
        		NodeDesc newnodeDesc;
        		int keysize = getKeySize(keyDesc.type,keyDesc.keyValue);

        		newnodeDesc.next=-1;
        		newnodeDesc.type=NonLeaf;
        		newnodeDesc.size=0;

        		//reuse void * page, copy keyDesc to the page
        		memcpy((char *)page+newnodeDesc.size,&keyDesc,sizeof(KeyDesc));
        		newnodeDesc.size+=sizeof(KeyDesc);
        		memcpy((char *)page+newnodeDesc.size,keyDesc.keyValue,keysize);
        		newnodeDesc.size+=keysize;

        		memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&newnodeDesc,sizeof(NodeDesc));

        		rc = ixfileHandle.writePage(newroot,page);
        		assert(rc == SUCCESS && "Fail to write root page as leaf page");
        		dprintf("Successfully split root page");
        	}



    	free(keyDesc.keyValue);
    	free(page);
    	dprintf("Original root page is NonLeaf");
    	return 0;
    }else{
    	assert("root page should be Leaf or NonLeaf");
    }
    // 
    
    return -1;
}

TreeOp IndexManager::insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc)
{
    TreeOp operation = OP_None;
    // retrieve node info
    NodeDesc nodeDesc;
    memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
    int offset = 0 ; bool insert = false;
    // potential split page buffer
    void *splitPage = malloc(PAGE_SIZE);

    // insert first 
    while( offset < nodeDesc.size ){
		    
	DataEntryDesc ded;
	memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );

	ded.keyValue = malloc(ded.keySize);
	memcpy( ded.keyValue, (char*)page+offset+DataEntryKeyOffset, ded.keySize); 
	
	// compare the key to find insertion point 
	int result = keyCompare(attribute, ded.keyValue, key);
	// key value smaller than rest of the data
	// insert new key,rid pair right here
	if( result > 0 ){
	    // use splitpage buffer as temp buffer, copy the rest of the key and rid lists
	    memcpy( splitPage , (char*)page+offset , nodeDesc.size - offset ); 
	    // insert a new <key,rid> pair
	    DataEntryDesc nDed;
	    nDed.numOfRID = 1;
	    nDed.overflow = false;
	    nDed.keySize = getKeySize(attribute,key); 
	    memcpy( (char*)page+offset, &nDed, sizeof(DataEntryDesc));
	    memcpy( (char*)page+offset+DataEntryKeyOffset, key , nDed.keySize ) ; 
	    memcpy( (char*)page+offset+DataEntryKeyOffset+nDed.keySize, &rid , sizeof(RID) );
	    // update offset and copy rest of the data back
	    offset += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
	    memcpy( (char*)page+offset, splitPage, nodeDesc.size - offset );
	    // update the node descriptor's size info
	    nodeDesc.size += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
	    memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );

	    free(ded.keyValue); 
	    insert = true;
	    break;
	}
	// same key value, append RID to the list
	if( result == 0 ){
	    // use splitpage buffer as temp buffer, copy the rest of the key and rid lists
	    memcpy( splitPage , (char*)page+offset , nodeDesc.size - offset ); 
	    // add RID to the back
	    memcpy( (char*)page+offset + sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID) , &rid , sizeof(RID));
	    memcpy( (char*)page+offset + sizeof(DataEntryDesc) + ded.keySize + (ded.numOfRID+1)*sizeof(RID) , splitPage , nodeDesc.size - offset );
	    // increase number of rid by 1 , write it back
	    ded.numOfRID++;
	    memcpy( (char*)page+offset, &ded , sizeof(DataEntryDesc) );
	    // update page descriptor  
	    nodeDesc.size += sizeof(RID);
	    memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc) , &nodeDesc , sizeof(NodeDesc) );
	    
	    free(ded.keyValue); 
	    insert = true;
	    break;
	}
	
	free(ded.keyValue); 
        offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);    
    }

    // if the key is the biggest in the page, append it
    if( !insert ){
	// insert a new <key,rid> pair
	DataEntryDesc nDed;
        nDed.numOfRID = 1;
        nDed.overflow = false;
        nDed.keySize = getKeySize(attribute,key); 
	memcpy( (char*)page+offset, &nDed, sizeof(DataEntryDesc));
        memcpy( (char*)page+offset+DataEntryKeyOffset, key , nDed.keySize ) ; 
        memcpy( (char*)page+offset+DataEntryKeyOffset+nDed.keySize, &rid , sizeof(RID) );

	// update the node descriptor's size info
	nodeDesc.size += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
        memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
    }

    // size > threshold , do split
    if( nodeDesc.size > THRESHOLD*1.7 ){
	offset = 0; // offset of entry which is going to split to right node
	operation = OP_Split;
	// add offset until it passes the half of size
	while( offset < nodeDesc.size / 2){    
	    DataEntryDesc ded;
	    memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );
	    offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);    

	}
	// allocate new page to insert splitted nodes
	PageNum freePageID = ixfileHandle.findFreePage();
	// form a new page, fill up the information 
	memcpy(splitPage, (char*)page+offset, nodeDesc.size - offset );
	NodeDesc splitNodeDesc;
	splitNodeDesc.type = Leaf;
	splitNodeDesc.size = nodeDesc.size - offset;
	splitNodeDesc.next = nodeDesc.next;
	splitNodeDesc.prev = pageNum;
	memcpy( (char*)splitPage+PAGE_SIZE-sizeof(NodeDesc), &splitNodeDesc, sizeof(NodeDesc) );
	nodeDesc.size = offset;
	nodeDesc.next = freePageID;
	memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );	

	ixfileHandle.writePage(freePageID,splitPage);

	// get First entry key value
	DataEntryDesc nDed;
	memcpy( &nDed, (char*)splitPage, sizeof(DataEntryDesc));
	keyDesc.rightNode = freePageID;
	keyDesc.leftNode = pageNum;
	keyDesc.keySize = nDed.keySize;
	memcpy( keyDesc.keyValue, (char*)splitPage+sizeof(DataEntryDesc), nDed.keySize);
    }
    
    ixfileHandle.writePage(pageNum,page); 
    return operation;    
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


TreeOp IndexManager::deleteFromLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page,
				    PageNum pageNum, KeyDesc &keyDesc)
{
    TreeOp operation = OP_None;
    // retrieve node info
    NodeDesc nodeDesc;
    memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
    int offset = 0 ; 
    // potential split page buffer
    void *nextPage = malloc(PAGE_SIZE);
    
    while( offset < nodeDesc.size ){
	DataEntryDesc ded;
	memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );

	ded.keyValue = malloc(ded.keySize);
	memcpy( ded.keyValue, (char*)page+offset+DataEntryKeyOffset, ded.keySize); 
	
	// compare the key to find the deleted record
	int result = keyCompare(attribute, ded.keyValue, key);

	// if it only contains 1 RID , remove whole entries
	if( result == 0 && ded.numOfRID == 1){
	    // use nextPage as temp buffer
	    int entrySize = sizeof(DataEntryDesc) + ded.keySize + sizeof(RID);
	    memcpy( nextPage, (char*)page+offset+entrySize , nodeDesc.size - ( offset + entrySize ) );
	    memcpy( (char*)page+offset , nextPage, nodeDesc.size - ( offset + entrySize ) );

	    nodeDesc.size -= entrySize;
	    memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
	    break;   
	}else{
	    // if it has more than two RIDs, remove the one in the list
	    
	    for( int i=0; i<ded.numOfRID; i++){
		RID t_rid;
		memcpy( &t_rid, (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*i, sizeof(RID) );
		if( rid.pageNum == t_rid.pageNum && rid.slotNum == t_rid.slotNum ){
		    int entrySize = sizeof(DataEntryDesc) + ded.keySize + sizeof(RID)*ded.numOfRID;
		    int restDataSize = (ded.numOfRID-(i+1))*sizeof(RID) + ( nodeDesc.size - offset - entrySize ) ;
		    memcpy( nextPage, (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*(i+1), restDataSize );
		    memcpy( (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+sizeof(RID)*i, nextPage, restDataSize );
		    ded.numOfRID -= 1;
		    memcpy( (char*)page+offset, &ded, sizeof(DataEntryDesc) );
		    nodeDesc.size -= sizeof(RID);
		    memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
		    break; // break for loop
		}
		
	    }
	    break; // break while loop
	}

	offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);

    }
 
    if( nodeDesc.size < THRESHOLD ){
	NodeDesc nNodeDesc;
	// right most leaf case 
	if( nodeDesc.next == -1 ){
	    ixfileHandle.readPage( nodeDesc.prev, nextPage );
	    memcpy( &nNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

	    // re-distribution case , else it needs to merge
	    if( nodeDesc.size + nNodeDesc.size > PAGE_SIZE ){
		offset = 0;
		while( offset < nNodeDesc.size / 3 * 2 ){
		    DataEntryDesc ded;
		    memcpy( &ded, (char*)nextPage+offset, sizeof(DataEntryDesc) );
		    offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
		}

		void *temp = malloc(PAGE_SIZE);
		memcpy( temp, page, nodeDesc.size );
		memcpy( page, (char*)nextPage+offset, nNodeDesc.size - offset );
		memcpy( (char*)page+offset, temp, nodeDesc.size );
		nodeDesc.size += ( nNodeDesc.size - offset );
		nNodeDesc.size = offset;
		free(temp);
	


		DataEntryDesc newKeyEntry;
		memcpy( &newKeyEntry, page, sizeof(DataEntryDesc) );
		memcpy( keyDesc.keyValue, (char*)page+sizeof(DataEntryDesc), newKeyEntry.keySize);
    
	    	keyDesc.rightNode = pageNum;
		keyDesc.leftNode = nodeDesc.prev;
		keyDesc.keySize = newKeyEntry.keySize;

	    }else{
		// merge case, nextPage is actually previous page
		memcpy( (char*)page+nodeDesc.size, nextPage, nNodeDesc.size );
		nodeDesc.size += nNodeDesc.size;

		ixfileHandle.deletePage( nodeDesc.prev );
		nodeDesc.prev = nNodeDesc.prev;
		memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
		operation = OP_Merge;
		ixfileHandle.writePage( pageNum , page );

	    }



	}else{
	    // normal case
	    ixfileHandle.readPage( nodeDesc.next, nextPage );
	    memcpy( &nNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	    
	    // re-distribution case , else it needs to merge
	    if( nodeDesc.size + nNodeDesc.size > PAGE_SIZE ){
		offset = 0;
		while( offset < nNodeDesc.size / 3 ){
		    DataEntryDesc ded;
		    memcpy( &ded, (char*)nextPage+offset, sizeof(DataEntryDesc) );
		    offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
		}
		memcpy( (char*)page+nodeDesc.size, nextPage, offset );
		void *temp = malloc(PAGE_SIZE);
		memcpy( temp, (char*)nextPage+offset, nNodeDesc.size - offset );
		memcpy( nextPage , temp , nNodeDesc.size - offset );
		nNodeDesc.size -= offset;
		nodeDesc.size += offset;
		free(temp);	
		DataEntryDesc newKeyEntry;
		memcpy( &newKeyEntry, nextPage, sizeof(DataEntryDesc) );
		memcpy( keyDesc.keyValue, (char*)nextPage+sizeof(DataEntryDesc), newKeyEntry.keySize);
	    	keyDesc.leftNode = pageNum;
		keyDesc.rightNode = nodeDesc.next;
		keyDesc.keySize = newKeyEntry.keySize;

	    }else{
		// merge case
		memcpy( (char*)page+nodeDesc.size, nextPage, nNodeDesc.size );
		nodeDesc.size += nNodeDesc.size;

		ixfileHandle.deletePage( nodeDesc.next );
		nodeDesc.next = nNodeDesc.next;
		memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
		operation = OP_Merge;
		ixfileHandle.writePage( pageNum, page );	    

	    }

	}

    }

    
	

    free(nextPage);
    return operation;
}

// Comparsion between two keys, 
// if KeyA greater than KeyB , return > 0 
// if KeyA smaller than KeyB , return < 0
// if eauqlity return 0
int IndexManager::keyCompare(const Attribute &attribute, const void *keyA, const void* keyB)
{
    AttrType type = attribute.type;

    int i_a , i_b;
    switch( type ){
	case TypeInt:
	    memcpy( &i_a , keyA , sizeof(int));
	    memcpy( &i_b , keyB , sizeof(int));
	    return (i_a - i_b);
	    break;
	case TypeReal:
	    float f_a, f_b;
	    memcpy( &f_a , keyA , sizeof(float));
	    memcpy( &f_b , keyB , sizeof(float));
	    f_a = (f_a-f_b) * 100000;
	    return (int)f_a;
	    break;
	case TypeVarChar:
	    memcpy( &i_a , keyA , sizeof(int));
	    memcpy( &i_b , keyB , sizeof(int));
	    string sa ((char*)keyA+sizeof(int),i_a);
	    string sb ((char*)keyB+sizeof(int),i_b);
	    return sa.compare(sb);
	    assert( false && "string comparsion under construction");
	    break;
    }

}

// Get Keysize 
int IndexManager::getKeySize(const Attribute &attribute, const void *key)
{
    AttrType type = attribute.type;
    int size = -1;
    switch( type ){
	case TypeInt:
	    return sizeof(int);
	case TypeReal:
	    return sizeof(float);
	case TypeVarChar:
	    memcpy( &size, key , sizeof(int) );
	    assert( size >= 0 && "something wrong with getting varchar key size\n");
	    assert( size < 50 && "something wrong with getting varchar key size\n");
	    return size+sizeof(int);
    }

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
    debug = true;

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

	NodeDesc nodeDesc;
	nodeDesc.next=-1;
	nodeDesc.type=Leaf;
	nodeDesc.size=0;
	memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
	rc = writePage(1,page);
	assert(rc == SUCCESS && "Fail to write root page as leaf page");

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
    this->readPageCount++;
    return fileHandle.readPage(pageNum,data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    this->writePageCount++;
    return fileHandle.writePage(pageNum,data);
}

RC IXFileHandle::deletePage(PageNum pageNum)
{
    PageNum dir = pageNum / IXDirectorySize;
    assert( dir % IXDirectorySize == 0 && "Not valid directory index\n" );
    int pageIndex = pageNum % IXDirectorySize;
    assert( pageIndex >= 1 && pageIndex < 1024 && "Not valid page index \n");

    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage( dir, page );
    // mark the slot as empty
    PageNum empty = 0;		
    memcpy( (char*)page+pageIndex , &empty , sizeof(PageNum) ); 
    fileHandle.writePage( dir, page );    
    free(page);

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
