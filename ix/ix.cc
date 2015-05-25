

#include <cmath>
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
		debug = true;
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
TreeOp IndexManager::TraverseTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page, PageNum pageNum, PageNum &returnpageNum)
{

	void *nextpage = malloc(PAGE_SIZE);
	NodeDesc nodeDesc;
	NodeDesc nextnodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));

	PageSize offset=0;
	KeyDesc currentkeyDesc;
	char *currentkeyValue = (char*)malloc(maxvarchar);
	PageNum currentpageNum=0;

	TreeOp treeop = OP_None;
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		//dprintf("currentkeyDesc.keySize is %d\n currentkeyDesc.leftNode is %d\n currentkeyDesc.rightNode is %d\n",currentkeyDesc.keySize,currentkeyDesc.leftNode,currentkeyDesc.rightNode);
		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;
		//dprintf("currentkeyValue is %d\n key is %d\n",currentkeyValue,key);

		if(keyCompare(attribute,key,currentkeyValue)<0){
			//get the page pointer
			currentpageNum=currentkeyDesc.leftNode;
			offset -= sizeof(KeyDesc);//adjust the offset for inserting a  key entry,
			offset -= currentkeyDesc.keySize;
			dprintf("keyCompare(attribute,key,currentkeyValue)<0\n currentpageNum is %d\n",currentpageNum);
			if( debug ) printKey(attribute,currentkeyValue);
			dprintf("\n");
			break;
		}
		if(offset == nodeDesc.size){
			//last entry
			currentpageNum=currentkeyDesc.rightNode;
			dprintf("offset == nodeDesc.size\n currentpageNum is %d\n",currentpageNum);
		 	if( debug ) printKey(attribute,currentkeyValue);
			dprintf("\n");

			break;
		}
	}
	currentkeyDesc.keyValue= currentkeyValue;
	dprintf("Before entering the next level,\ncurrentpageNUm is %d\n",currentpageNum);
	ixfileHandle.readPage(currentpageNum,nextpage);
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	if(nextnodeDesc.type == Leaf){
		dprintf("nextnodeDesc.type == Leaf\n");
		returnpageNum=currentpageNum;
		treeop=OP_None;
		free(nextpage);
		free(currentkeyValue);
		return treeop;
	}else if(nextnodeDesc.type == NonLeaf){
		dprintf("nextnodeDesc.type == NonLeaf\n");
		TraverseTree(ixfileHandle, attribute, key, nextpage, currentpageNum, returnpageNum);
		treeop=OP_None;
		free(nextpage);
		free(currentkeyValue);
		return treeop;

	}else{
		assert(false &&"nextnodeDesc.type should be Leaf or Nonleaf");
	}

}


TreeOp IndexManager::TraverseTreeInsert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc)
{
	void *bufferpage = malloc(PAGE_SIZE);
	void *nextpage = malloc(PAGE_SIZE);
	void *buffer = malloc(PAGE_SIZE);
	void *keyValue = keyDesc.keyValue;
	void *extrapage = malloc(PAGE_SIZE);
	assert( keyValue == keyDesc.keyValue && " begining keyValue should equal to keyDesc.keyValue \n");
	NodeDesc nodeDesc;
	NodeDesc nextnodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	NodeDesc extranodeDesc;
	PageSize offset=0;
	KeyDesc siblingkeyDesc;
	KeyDesc currentkeyDesc;
	char *currentkeyValue = (char *)malloc(maxvarchar);
	KeyDesc nextkeyDesc;
	nextkeyDesc.keyValue = malloc(maxvarchar);
	PageNum currentpageNum=0;


	TreeOp treeop = OP_None;
	TreeOp nexttreeop = OP_None;

	//scan to find the desired pointer
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;
		//dprintf("currentkeyValue is %d\n key is %d\n",currentkeyValue,key);
		if(keyCompare(attribute,key,currentkeyValue)<0){
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
	currentkeyDesc.keyValue = currentkeyValue;
	assert( currentpageNum != 0 && "Should find a pageNum\n");


	int splitoffset=0;
	NodeDesc tempnodeDesc;
	PageSize origsize=nodeDesc.size;
	KeyDesc tempkeyDesc;
	int tempnext=nodeDesc.next;
	if(nodeDesc.size >= UpperThreshold){
		//split the page
		dprintf("before split, nodeDesc.size is %d\n",nodeDesc.size);

		//find the position to split
		while(true){

			//use keyDesc
			memcpy(&tempkeyDesc,(char *) page+splitoffset,sizeof(KeyDesc));
			splitoffset += sizeof(KeyDesc);
			splitoffset += tempkeyDesc.keySize;


			if(splitoffset >= UpperThreshold/2){

				break;
			}
		}

		//create nodeDesc for original page

		nodeDesc.size = splitoffset;
		nodeDesc.next = ixfileHandle.findFreePage();


		//push up a key value
		memcpy(&tempkeyDesc,(char *) page+splitoffset,sizeof(KeyDesc));
		splitoffset += sizeof(KeyDesc);
		//updating keySize to returned key
		keyDesc.keySize = tempkeyDesc.keySize;
		dprintf("keyDesc.keySize is %d\n",keyDesc.keySize);
		memcpy(keyDesc.keyValue,(char *) page+splitoffset,keyDesc.keySize);
		splitoffset += keyDesc.keySize;
		keyDesc.leftNode = pageNum;
		keyDesc.rightNode = nodeDesc.next;

		//create nodeDesc for new page
		tempnodeDesc.size = origsize-splitoffset;
		tempnodeDesc.prev = pageNum;
		tempnodeDesc.next = tempnext;
		tempnodeDesc.type = NonLeaf;
		assert(splitoffset < origsize && "splitoffset should be less than origsize\n");

		//move data to new page
		memcpy(bufferpage,(char *)page+splitoffset,origsize-splitoffset);
		//update nodeDesc for  two pages
		memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
		memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
		//write these pages to disk
		ixfileHandle.writePage(pageNum,page);
		ixfileHandle.writePage(nodeDesc.next,bufferpage);
		treeop = OP_Split;
		dprintf("treeop is split\nafter split, nodeDesc.size is %d\n",nodeDesc.size);
		if( tempnodeDesc.next != InvalidPage ){
			//read new right sibling page
			ixfileHandle.readPage(tempnodeDesc.next,extrapage);
			//update nodeDesc in new right sibling
			memcpy(&extranodeDesc,(char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
			extranodeDesc.prev = nodeDesc.next;
			memcpy((char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),&extranodeDesc,sizeof(NodeDesc));
			//write page to disk
			ixfileHandle.writePage(tempnodeDesc.next,extrapage);


		}


	}

	RC rc;
	//recursively call TraverseTreeInsert
	dprintf("currentpageNUm is %d\n",currentpageNum);
	//read the page pointed by currentpageNum to nextpage
	rc = ixfileHandle.readPage(currentpageNum,nextpage);
	assert(rc == 0 && "rc != 0 \n");
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	//if it is leaf page call insertToLeat, NonLeaf page recursively call TraverseTreeInsert

	if(nextnodeDesc.type == Leaf){
		nexttreeop = insertToLeaf(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, nextkeyDesc);

		assert((nexttreeop == OP_Split || nexttreeop == OP_None) && "nexttreeop should be OP_split or OP_None");

	}else if(nextnodeDesc.type == NonLeaf){


		nexttreeop = TraverseTreeInsert(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, nextkeyDesc);
		assert((nexttreeop == OP_Split || nexttreeop == OP_None) && "nexttreeop should be OP_split or OP_None");


	}else{
		assert("page type should be leaf or NonLeaf");
	}
	assert( keyValue == keyDesc.keyValue && "after recursive call, keyValue should equal to keyDesc.keyValue \n");
	if(nexttreeop == OP_Split){
		dprintf("nextteeop is OP_Split\n");
		if(treeop == OP_Split ){

			dprintf("teeop is OP_Split\n");

			if(offset == nodeDesc.size){
				//When offset == nodeDesc.size ,the offset didn't include pushed up key
				//offset should be 0
				offset -= nodeDesc.size;
				dprintf("offset == nodeDesc.size\n");

				//update sibling KeyDesc(the first key in new page)
				memcpy(&siblingkeyDesc,(char *)bufferpage+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)bufferpage+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//may cause problem,move data backward
				memcpy(buffer,(char *)bufferpage+offset,tempnodeDesc.size-offset);
				dprintf("nextkeyDesc.keySize is %d\n",nextkeyDesc.keySize);
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,buffer,tempnodeDesc.size-offset);
				memcpy((char *)bufferpage+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);

				//update nodeDesc for new page, write page to disk
				tempnodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(nodeDesc.next,bufferpage);



			}else if(offset > nodeDesc.size){
				//offset include the pushed up  key
				dprintf("offset > nodeDesc.size\n");

				offset -= nodeDesc.size;
				offset -= sizeof(KeyDesc);
				offset -= keyDesc.keySize;

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)bufferpage+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)bufferpage+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//may cause problem,move data backward
				memcpy(buffer,(char *)bufferpage+offset,tempnodeDesc.size-offset);
				dprintf("nextkeyDesc.keySize is %d\n",nextkeyDesc.keySize);

				memcpy((char *)bufferpage+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,(char *)buffer,tempnodeDesc.size-offset);
				//copy the inserted key to bufferpage
				memcpy((char *)bufferpage+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)bufferpage+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);
				//update nodeDesc for new page, write page to disk
				tempnodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)bufferpage+PAGE_SIZE-sizeof(NodeDesc),&tempnodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(nodeDesc.next,bufferpage);

			}else{
				dprintf("offset < nodeDesc.size\n");
				assert(offset < nodeDesc.size && "offset < nodeDesc.size");
				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));

				//move data backward
				memcpy(buffer,(char *)page+offset,nodeDesc.size-offset);
				dprintf("nextkeyDesc.keySize is %d\n",nextkeyDesc.keySize);

				memcpy((char *)page+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,buffer,nodeDesc.size-offset);
				//copy the inserted key to page
				memcpy((char *)page+offset,&nextkeyDesc,sizeof(KeyDesc));
				memcpy((char *)page+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);
				//update nodeDesc for new page, write page to disk
				nodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				ixfileHandle.writePage(pageNum,page);

			}

		}else if(treeop == OP_None){
			dprintf("teeop is OP_None\n");
			//update sibling KeyDesc

			memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
			siblingkeyDesc.leftNode = nextkeyDesc.rightNode;
			memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));


			//move data backward
			memcpy(buffer,(char *)page+offset,nodeDesc.size-offset);
			memcpy((char *)page+offset+sizeof(KeyDesc)+nextkeyDesc.keySize,buffer,nodeDesc.size-offset);
			//copy the inserted key to page

			memcpy((char *)page+offset,&nextkeyDesc,sizeof(KeyDesc));
			memcpy((char *)page+offset+sizeof(KeyDesc),nextkeyDesc.keyValue,nextkeyDesc.keySize);
			//update nodeDesc for new page, write page to disk

			nodeDesc.size += (sizeof(KeyDesc) + nextkeyDesc.keySize);
			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
			ixfileHandle.writePage(pageNum,page);

		}else{
			assert("treeop should be OP_split or OP_None");
		}
	}

	free(extrapage);
	free(buffer);
	free(nextpage);
	free(bufferpage);
	free(currentkeyDesc.keyValue);
	free(nextkeyDesc.keyValue);
	if(nexttreeop == OP_Error){
		treeop = nexttreeop;
	}
	assert( keyValue == keyDesc.keyValue && "keyValue should equal to keyDesc.keyValue ");
	checkKeyInt(ixfileHandle, attribute, page);
	checkPageInt(ixfileHandle, page, pageNum,false);
	return treeop;
}
void IndexManager::checkPageInt(IXFileHandle &ixfileHandle, void *page,PageNum pageNum,bool p)
{
	void *rightsibling = malloc(PAGE_SIZE);
	void *leftsibling = malloc(PAGE_SIZE);
	PageNum prevpageNum = InvalidPage;
	//initialize pageNum
	PageNum currentpageNum = pageNum;
	PageNum nextpageNum = InvalidPage;
	//initialize currentnodeDesc
	NodeDesc currentnodeDesc;
	memcpy(&currentnodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	NodeDesc rightnodeDesc;
	NodeDesc leftnodeDesc;


	//To right way to check double link list
	while(currentnodeDesc.next != InvalidPage){

		//read the right sibling page
		ixfileHandle.readPage(currentnodeDesc.next,rightsibling);

		//get the sibling nodeDesc
		memcpy(&rightnodeDesc,(char *)rightsibling+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));

		//compare prev to currentpageNum
		if(currentpageNum != rightnodeDesc.prev){
			dprintf("currentpageNum is %d\nnextpageNum is %d\nrightnodeDesc.prev is %d\n",currentpageNum,currentnodeDesc.next,rightnodeDesc.prev);
			assert(currentpageNum == rightnodeDesc.prev);
		}
		//updata the current nodeDesc as the sibling nodeDesc
		currentpageNum = currentnodeDesc.next;
		currentnodeDesc = rightnodeDesc;

	}
	//reinitialize currentnodeDesc and currentpageNum
	memcpy(&currentnodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	currentpageNum = pageNum;
	//To left way to check double link list
	while(currentnodeDesc.prev != InvalidPage){

		//read the left sibling page
		ixfileHandle.readPage(currentnodeDesc.prev,leftsibling);

		//get the sibling nodeDesc
		memcpy(&leftnodeDesc,(char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
		if(p){
		//dprintf("currentpageNum is %d\ncurrentnodeDesc.prev is %d\nleftnodeDesc.next is %d\n",currentpageNum,currentnodeDesc.prev,leftnodeDesc.next);
		}
		//compare prev to currentpageNum
		if(currentpageNum != leftnodeDesc.next){
			dprintf("currentpageNum is %d\nprevpageNum is %d\nlefttnodeDesc.next is %d\n",currentpageNum,currentnodeDesc.prev,leftnodeDesc.next);
			assert(currentpageNum == leftnodeDesc.next);
		}
		//updata the current nodeDesc as the sibling nodeDesc
		currentpageNum = currentnodeDesc.prev;
		currentnodeDesc = leftnodeDesc;

	}

	free(rightsibling);
	free(leftsibling);

}

void IndexManager::checkKeyInt(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page)
{
	KeyDesc currentkeyDesc;
	currentkeyDesc.keyValue = malloc(maxvarchar);
	void * currentkeyValue = currentkeyDesc.keyValue;

	//initialize rightnode
	PageNum rightnode;
	KeyDesc checkkeyDesc;
	memcpy(&checkkeyDesc,page,sizeof(KeyDesc));
	rightnode = checkkeyDesc.leftNode;
	int offset = 0;
	int oldoffset;
	NodeDesc nodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	if(nodeDesc.size == 0){
		dprintf("this page is empty page\n");
		return;
	}
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;
		if(rightnode != currentkeyDesc.leftNode){
			//printKey(attribute,currentkeyValue);
			dprintf("rightnode is %d\n curentkeyDesc.leftNode is %d\n oldoffset is %d\n",rightnode,currentkeyDesc.leftNode,oldoffset);
			assert(rightnode == currentkeyDesc.leftNode && "rightnode ==currentkeyDesc.leftNode");
		}

		if(offset == nodeDesc.size){
			//last entry
			break;
		}

		oldoffset = offset ;//adjust the offset for deleting a  key entry,
		rightnode = currentkeyDesc.rightNode;
	}
	free(currentkeyValue);

}


RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	// find root first
	dprintf("in insertEntry\n");

	PageNum root = ixfileHandle.findRootPage();
	void *page = malloc(PAGE_SIZE);
	dprintf("root pageNum is %d\n",root);
	rc = ixfileHandle.readPage(root,page);

	KeyDesc keyDesc;
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
			dprintf("in insert Entry Leaf\n newroot is %d\n",newroot);
			ixfileHandle.updateRootPage(newroot);
			NodeDesc newnodeDesc;
			int keysize = getKeySize(attribute,keyDesc.keyValue);

			newnodeDesc.next=InvalidPage;
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
			dprintf("Successfully split root page\n");
		}
		free(keyDesc.keyValue);
		free(page);
		dprintf("Original root page is Leaf yo\n");
		return 0;
	}else if(type == NonLeaf){
		//root page is NonLeaf

		TreeOp treeop=TraverseTreeInsert(ixfileHandle, attribute, key, rid, page, root, keyDesc);
		assert( ((treeop == OP_Split) || (treeop == OP_None)) && "treeop should be OP_Split or OP_None"  );
		if(treeop == OP_Split){
			PageNum newroot;
			newroot=ixfileHandle.findFreePage();
			dprintf("in insert Entry NonLeaf\n newroot is %d\n",newroot);
			ixfileHandle.updateRootPage(newroot);
			NodeDesc newnodeDesc;
			int keysize = getKeySize(attribute,keyDesc.keyValue);

			newnodeDesc.next=InvalidPage;
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

		free(page);
		free(keyDesc.keyValue);
		dprintf("Original root page is NonLeaf\n");
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
//		if( result == 0 && ded.keySize-4 != *(int*)key ) assert(false);
//if( (ded.keySize-4) == *(int*)key ) { 
	//	printf("result %d ",result); printKey(attribute,ded.keyValue); printf(" "); printKey(attribute,key); printf("\n");
	//	printf("%d %d %d\n",ded.keySize-4,*(int*)key,offset); // }

		// key value smaller than rest of the data
		// insert new key,rid pair right here
		if( result > 0 ){ 
//			printf("%d %d\n",*(int*)key,*(int*)ded.keyValue);
			// use splitpage buffer as temp buffer, copy the rest of the key and rid lists
			int restDataSize = nodeDesc.size - offset;
			memcpy( splitPage , (char*)page+offset , restDataSize );
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
			memcpy( (char*)page+offset, splitPage, restDataSize );
			// update the node descriptor's size info
			nodeDesc.size += sizeof(DataEntryDesc) + nDed.keySize + nDed.numOfRID * sizeof(RID);
			memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );

			free(ded.keyValue);
			insert = true;
			break;
		}
		// if the keypair is overflowed, insert it to overflow page
		if( result == 0 && ded.overflow != InvalidPage ){
			RC rc;
	    printf("Hi\n"); assert(false);
			rc = ixfileHandle.readPage( ded.overflow , page );
			assert( rc == SUCCESS && "read overflow page failed");
			memcpy( &ded, page, sizeof(DataEntryDesc));
			memcpy( (char*)page+sizeof(DataEntryDesc)+ded.keySize+ded.numOfRID*sizeof(RID), &rid, sizeof(RID) );
			ded.numOfRID++;
			memcpy( page, &ded, sizeof(DataEntryDesc));
			assert( sizeof(DataEntryDesc)+ded.keySize+ded.numOfRID*sizeof(RID) < PAGE_SIZE && "overflow page overflowed" );
			rc = ixfileHandle.writePage( ded.overflow , page );
			assert( rc == SUCCESS && "write overflow page failed");
			free(ded.keyValue);
			return operation;
		}
		// check if the RID list is too big so that we need to move it to a overflow page.
		// the condition depends on RID list's size bigger than LowerThreshold Bound

		if( result == 0 && ded.numOfRID*sizeof(RID) > LowerThreshold ){
	    printf("Hi\n"); assert(false);
			// find over flow page and update the current ded's overflow page indicator
			PageNum link = ixfileHandle.findFreePage(); 
			ded.overflow = link; // update overflow indicator
			memcpy( (char*)page+offset, &ded , sizeof(DataEntryDesc) ); // update ded to original page
			// put new key,rid pair into over flow page
			void *overflowPage = malloc(PAGE_SIZE); // create another overflow page
			ded.numOfRID = 1; ded.overflow = link; // same ded but different size of RID list,
			// insert RID and ded to overflow page
			memcpy( overflowPage, &ded, sizeof(DataEntryDesc) );
			memcpy( (char*)overflowPage+sizeof(DataEntryDesc)+ded.keySize, &rid ,sizeof(RID));
			RC rc; 
			rc = ixfileHandle.writePage( ded.overflow , overflowPage );
			assert( rc == SUCCESS && "write overflow page failed");

			free(overflowPage);
			free(ded.keyValue);
			return operation;
		}

		// same key value, append RID to the list
		if( result == 0 ){ 
			int pairSize = sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
			// use splitpage buffer as temp buffer, copy the rest of the key and rid lists
			memcpy( splitPage , (char*)page+offset+pairSize , nodeDesc.size - ( offset + pairSize ) );
			// add RID to the back
			memcpy( (char*)page+offset + sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID) , &rid , sizeof(RID));
			memcpy( (char*)page+offset + sizeof(DataEntryDesc) + ded.keySize + (ded.numOfRID+1)*sizeof(RID) , splitPage , nodeDesc.size - (offset + pairSize ) );
			// increase number of rid by 1 , write it back
			ded.numOfRID++;
			memcpy( (char*)page+offset, &ded , sizeof(DataEntryDesc) );
			// update page descriptor
			nodeDesc.size += sizeof(RID);
			memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc) , &nodeDesc , sizeof(NodeDesc) );
if( *(int*)key == 20 ) printf("%d %d %d %d\n", rid.pageNum, ded.numOfRID, pageNum, nodeDesc.size);
			free(ded.keyValue);
			insert = true;
			break;
		}

		free(ded.keyValue);
		offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
	} // end while of searching insertion point


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
	if( nodeDesc.size > UpperThreshold ){ printf("YOLO\n");
		offset = 0; // offset of entry which is going to split to right node
		operation = OP_Split;
		// add offset until it passes the half of size
		while( offset < nodeDesc.size / 2){
			DataEntryDesc ded;
			memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );
			offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
if( ded.numOfRID > 110 ) printf("Hiiii %d\n",ded.keySize);
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
		// update right leaf's left node to split page number 
		// if it's not empty
		if( splitNodeDesc.next != InvalidPage ){
		    NodeDesc ntNodeDesc;
		    ixfileHandle.readPage( splitNodeDesc.next, splitPage);
		    memcpy( &ntNodeDesc, (char*)splitPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc));
		    ntNodeDesc.prev = freePageID; 
		    memcpy( (char*)splitPage+PAGE_SIZE-sizeof(NodeDesc), &ntNodeDesc, sizeof(NodeDesc));
		    ixfileHandle.writePage( splitNodeDesc.next, splitPage);
		}
		// get First entry key value
		DataEntryDesc nDed;
		memcpy( &nDed, (char*)splitPage, sizeof(DataEntryDesc));
		keyDesc.rightNode = freePageID;
		keyDesc.leftNode = pageNum;
		keyDesc.keySize = nDed.keySize;
		memcpy( keyDesc.keyValue, (char*)splitPage+sizeof(DataEntryDesc), nDed.keySize);

	}

	ixfileHandle.writePage(pageNum,page);
	free(splitPage); 
	return operation;
}
void IndexManager::FindLastKey(void *page,KeyDesc &keyDesc)
{
	NodeDesc nodeDesc;
	int offset = 0;
	memcpy(&nodeDesc,(char*)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	while(true){
		memcpy(&keyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=(sizeof(KeyDesc) + keyDesc.keySize);
		if(offset == nodeDesc.size){
			break;
		}
	}
}
RC IndexManager::FindOffset(void *page,int size,int &offset,bool IsGreater)
{
	KeyDesc keyDesc;
	offset = 0;
	int oldoffset = -1;
	NodeDesc nodeDesc;
	memcpy(&nodeDesc,(char*)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));

	while(true){
		memcpy(&keyDesc,(char *)page+offset,sizeof(keyDesc));
		offset += (sizeof(KeyDesc)+keyDesc.keySize);
		if(IsGreater){
			if(offset >= size){
				return 0;
			}else if(offset > nodeDesc.size){
				return -1;
			}
		}else{
			if(offset >= size){
				offset = oldoffset;
				return 0;
			}else if(offset > nodeDesc.size){
				return -1;
			}
		}
		oldoffset = offset;
	}
}


TreeOp IndexManager::TraverseTreeDelete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum pageNum, KeyDesc &keyDesc, int  rightMost )
{
	void *bufferpage = malloc(PAGE_SIZE);
	void *nextpage = malloc(PAGE_SIZE);
	void *leftsibling = malloc(PAGE_SIZE);
	void *rightsibling = malloc(PAGE_SIZE);
	void *extrapage = malloc(PAGE_SIZE);
	void *deletepage = malloc(PAGE_SIZE);
	NodeDesc nodeDesc;
	NodeDesc tempnodeDesc;
	NodeDesc nextnodeDesc;
	NodeDesc extranodeDesc;
	NodeDesc deletenodeDesc;
	memcpy(&nodeDesc,(char *)page+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	PageNum tempnext = nodeDesc.next;
	PageNum tempprev = nodeDesc.prev;
	int pushdownkeysize = keyDesc.keySize;
	int nextRightmost = 0;
	PageSize offset=0;


	PageSize oldoffset=0;
	KeyDesc siblingkeyDesc;
	KeyDesc currentkeyDesc;
	KeyDesc deletedkeyDesc;
	KeyDesc TobedeletekeyDesc;
	void * keyValue = keyDesc.keyValue;
	currentkeyDesc.keyValue = malloc(maxvarchar);
	void * currentkeyValue = currentkeyDesc.keyValue;
	KeyDesc nextkeyDesc;
	nextkeyDesc.keyValue = malloc(maxvarchar);
	void *nextkeyValue = nextkeyDesc.keyValue;
	PageNum currentpageNum=0;
	//initialize rightnode
	PageNum rightnode;
	KeyDesc checkkeyDesc;
	memcpy(&checkkeyDesc,page,sizeof(KeyDesc));
	rightnode = checkkeyDesc.leftNode;

	TreeOp treeop = OP_None;
	TreeOp nexttreeop = OP_None;
	//IsRightest to indicate whtether the currentpageNum is the rightNode
	bool IsRightest = false;
	bool DeleteNext = false;
	//store the leftNode and rightNode in currentkeyDesc
	PageNum temprightNode = 0;
	PageNum templeftNode =0;
	

	//scan to find the desired pointer
	while(true){
		memcpy(&currentkeyDesc,(char *) page+offset,sizeof(KeyDesc));
		offset+=sizeof(KeyDesc);
		memcpy(currentkeyValue,(char *) page+offset,currentkeyDesc.keySize);
		offset+=currentkeyDesc.keySize;
		if(keyCompare(attribute,key,currentkeyValue)<0){
			//get the page pointer

			currentpageNum=currentkeyDesc.leftNode;
			dprintf("keyCompare(attribute,key,currentkeyValue)<0,\nrightnode is %d\ncurrentpageNUm is %d\n",rightnode,currentpageNum);
			if( debug ) printKey(attribute,currentkeyValue);
			dprintf("\n");
			assert(rightnode == currentkeyDesc.leftNode && "compare < 0,currentpageNum=currentkeyDesc.leftNode");
			break;
		}

		if(offset == nodeDesc.size){
			//last entry
			currentpageNum=currentkeyDesc.rightNode;
			dprintf("offset == nodeDesc.size,\nrightnode is %d\ncurrentpageNUm is %d\n",rightnode,currentpageNum);
			if( debug ) printKey(attribute,currentkeyValue);
			dprintf("\n");
			assert(rightnode == currentkeyDesc.leftNode && "In the end,currentpageNum=currentkeyDesc.leftNode");
			IsRightest = true;
			nextRightmost = 1;

			break;
		}

		rightnode = currentkeyDesc.rightNode;
	}
	oldoffset = offset - sizeof(KeyDesc) - currentkeyDesc.keySize;
	dprintf("after while loop, offset is %d\n oldoffset is %d\n",offset,oldoffset);
	currentkeyDesc.keyValue = currentkeyValue;
	//store leftNode and rightNode in currentkeyDesc
	temprightNode = currentkeyDesc.rightNode;
	templeftNode = currentkeyDesc.leftNode;

	assert( currentpageNum != -1 && "Should find a pageNum");
//	printf("after finding currentpageNum,oldoffset is %d\n offset is %d\n",oldoffset,offset); 
	//assert(false);

	NodeDesc leftnodeDesc;
	NodeDesc rightnodeDesc;
	PageSize origsize=nodeDesc.size;
	PageSize leftsize=0;
	PageSize rightsize=0;
	KeyDesc lastkeyDesc;
	KeyDesc beginkeyDesc;
	int currentoffset = nodeDesc.size;
	bool Emptyflag = false;

	if(nodeDesc.size < LowerThreshold){
		//merege or redistribute the page


		if(nodeDesc.prev != InvalidPage || rightMost != 1){

			//not root page


			if(rightMost != 1){




				ixfileHandle.readPage(nodeDesc.next,rightsibling);
				memcpy(&rightnodeDesc,(char*)rightsibling+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
				dprintf("rightnodeDesc.size is %d\n",rightnodeDesc.size);
				dprintf("In the begining , nodeDesc.size is %d\n",nodeDesc.size);

				if( (rightnodeDesc.size + nodeDesc.size)> UpperThreshold){
					//redistribute data from rightsibling
					checkKeyInt(ixfileHandle, attribute, page);
					dprintf("IN begining\n");

					int siblingoffset;
					//find the position in sibling page to move data
					FindOffset(rightsibling,LowerThreshold-nodeDesc.size,siblingoffset,true);
					dprintf("nodeDesc.size is %d\nsiblingoffset is %d\n",nodeDesc.size,siblingoffset);
					//find the last key in original page and the first key in sibling page
					FindLastKey(page,lastkeyDesc);
					assert(lastkeyDesc.rightNode != InvalidPage && "rightNode != InvalidPage in Redistribution");
					memcpy(&beginkeyDesc,rightsibling,sizeof(KeyDesc));
					//update the leftNode and rightNode for push down key
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;
					dprintf("keyDesc.leftNode is %d\nkeyDesc.righttNode is %d\n",keyDesc.leftNode,keyDesc.rightNode);
					//printKey(attribute,keyValue);
					//push down key
					memcpy((char *)page+currentoffset,&keyDesc,sizeof(KeyDesc));
					currentoffset += sizeof(KeyDesc);
					memcpy((char *)page+currentoffset,keyDesc.keyValue,keyDesc.keySize);
					currentoffset += keyDesc.keySize;
					//move the data from beginning to the splitoffset in  rightsibling to original page
					//redistribute data from right page
					memcpy((char *)page+currentoffset,(char *)rightsibling,siblingoffset);
					currentoffset += siblingoffset;
					dprintf("siblingoffset is %d\n",siblingoffset);
					//update nodeDesc
					nodeDesc.size = currentoffset;
					dprintf("In the end , nodeDesc.size is %d\n",nodeDesc.size);

					memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
					checkKeyInt(ixfileHandle, attribute, page);

					//create returned keyDesc from the first key in sibling page
					memcpy(&keyDesc,(char*)rightsibling+siblingoffset,sizeof(KeyDesc));
					keyDesc.keyValue = keyValue;
					siblingoffset += sizeof(KeyDesc);
					memcpy(keyDesc.keyValue,(char*)rightsibling+siblingoffset,keyDesc.keySize);
					siblingoffset += keyDesc.keySize;
					//move entry in right sibling
					memcpy(bufferpage,(char*)rightsibling+siblingoffset,rightnodeDesc.size-siblingoffset);
					assert((rightnodeDesc.size-siblingoffset)>= 0 && "rightnodeDesc.size-siblingoffset >= 0");
					memcpy(rightsibling,(char*)bufferpage,rightnodeDesc.size-siblingoffset);
					//update rightnodeDesc
					rightnodeDesc.size = rightnodeDesc.size - siblingoffset;
					dprintf("rightnodeDesc.size is %d\n",rightnodeDesc.size);
					memcpy((char *)rightsibling+PAGE_SIZE-sizeof(NodeDesc),&rightnodeDesc,sizeof(NodeDesc));

					//update page and rigntsibling to disk
					ixfileHandle.writePage(pageNum,page);
					ixfileHandle.writePage(nodeDesc.next,rightsibling);
					treeop = OP_Dist;
					dprintf("treeop is OP_DIST, Not the rightest one\n");

				}else{
					//merge, move data from right sibling
					int tempnext = nodeDesc.next;
					//find the last key in original page and the first key in sibling page
					FindLastKey(page,lastkeyDesc);
					memcpy(&beginkeyDesc,rightsibling,sizeof(KeyDesc));
					//update the leftNode and rightNode for push down key
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;
					//push down key
					memcpy((char *)page+nodeDesc.size,&keyDesc,sizeof(keyDesc));
					nodeDesc.size += sizeof(keyDesc);
					memcpy((char *)page+nodeDesc.size,keyDesc.keyValue,keyDesc.keySize);
					nodeDesc.size += keyDesc.keySize;
					//move data from rightsibling to original page
					memcpy((char *)page+nodeDesc.size,(char *)rightsibling,rightnodeDesc.size);
					nodeDesc.size += rightnodeDesc.size;
					nodeDesc.next =rightnodeDesc.next;
					//update nodeDesc
					memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
					dprintf("Merge, the new nodeDesc.size is %d\n",nodeDesc.size);
					//write page to disk and delete the rightsibling in disk
					ixfileHandle.writePage(pageNum,page);
					assert( ixfileHandle.findRootPage() != tempnext && "shouldn't delete root page");
					ixfileHandle.deletePage(tempnext);
					treeop = OP_Merge;
					dprintf("treeop is OP_Merge, Not the rightest one\n");
					//update nodeDesc of new right sibling page
					if( nodeDesc.next != InvalidPage ){
						//read new right sibling page
						ixfileHandle.readPage(nodeDesc.next,extrapage);
						//update nodeDesc in new right sibling
						memcpy(&extranodeDesc,(char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
						extranodeDesc.prev = pageNum;
						memcpy((char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),&extranodeDesc,sizeof(NodeDesc));
						//write page to disk
						ixfileHandle.writePage(nodeDesc.next,extrapage);


					}


				}

			}else{
				//If current page is the rightest page, move data from left page
				int backwardoffset = 0;
				//get the leftnodeDesc from leftsibling
				ixfileHandle.readPage(nodeDesc.prev,leftsibling);
				memcpy(&leftnodeDesc,(char*)leftsibling+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
				dprintf("leftsize is %d\n",leftnodeDesc.size);
				dprintf("In the begining , nodeDesc.size is %d\n",nodeDesc.size);

				if( (leftnodeDesc.size + nodeDesc.size)> UpperThreshold){
					int siblingoffset;
					//find the begining point in left page to move data
					FindOffset(leftsibling,leftnodeDesc.size - (LowerThreshold-nodeDesc.size),siblingoffset,false);
					assert((leftnodeDesc.size - (LowerThreshold-nodeDesc.size))>LowerThreshold && "(leftNode.size - (LowerThreshold-nodeDesc.size))>LowerThreshold");
					dprintf("nodeDesc.size is %d\n",nodeDesc.size);
					dprintf("leftNode.size is %d\n",leftnodeDesc.size);
					dprintf("siblingoffset is %d\n",siblingoffset);
					//find the last key in original page and the first key in sibling page
					FindLastKey(leftsibling,lastkeyDesc);
					assert(lastkeyDesc.rightNode != InvalidPage && "rightNode == InvalidPage in Redistribution");
					memcpy(&beginkeyDesc,page,sizeof(KeyDesc));
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;

					//push down key
					backwardoffset = keyDesc.keySize+sizeof(KeyDesc);
					//move the data in page backward
					memcpy(bufferpage,page,currentoffset);
					memcpy((char *)page+backwardoffset,bufferpage,currentoffset);
					//copy push down key to page
					memcpy((char *)page,&keyDesc,sizeof(KeyDesc));
					currentoffset += sizeof(KeyDesc);
					oldoffset += backwardoffset;
					offset += backwardoffset;
					memcpy((char *)page+sizeof(KeyDesc),keyDesc.keyValue,keyDesc.keySize);
					currentoffset += keyDesc.keySize;


					assert(backwardoffset == (currentoffset - nodeDesc.size) &&"backwardoffset == (sizeof(KeyDesc) + keyDesc.keySize)");


					//move the data in page backward
					backwardoffset = leftnodeDesc.size - siblingoffset;
					memcpy(bufferpage,page,currentoffset);
					memcpy((char *)page+backwardoffset,bufferpage,currentoffset);
					//move data starting in siblingoffset from leftsibling to the page
					memcpy((char *)page,(char *)leftsibling+siblingoffset,backwardoffset);//redistribute data from right page
					currentoffset += backwardoffset;
					oldoffset += backwardoffset;
					offset += backwardoffset;
					dprintf("nodeDesc.size is %d\n",nodeDesc.size);
					dprintf("leftNode.size is %d\n",leftnodeDesc.size);
					dprintf("siblingoffset is %d\n",siblingoffset);
					//update nodeDesc
					nodeDesc.size = currentoffset;
					dprintf("In the end , nodeDesc.size is %d\n",nodeDesc.size);
					memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));

					//create returned keyDesc
					//modify leftnodeDesc.size for  FindLastKey to use
					leftnodeDesc.size = siblingoffset;
					memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
					FindLastKey(leftsibling,lastkeyDesc);
					//pointing to the start position for last key
					leftnodeDesc.size -= (sizeof(KeyDesc) + lastkeyDesc.keySize);
					//copy the keyDesc to returned key
					memcpy(&keyDesc,(char*)leftsibling+leftnodeDesc.size,sizeof(KeyDesc));
					keyDesc.keyValue = keyValue;
					memcpy(keyDesc.keyValue,(char*)leftsibling+leftnodeDesc.size+sizeof(KeyDesc),keyDesc.keySize);

					//update leftnodeDesc
					dprintf("leftnodeDesc.size is %d\n",leftnodeDesc.size);
					memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));

					//update page and rigntsibling to disk
					ixfileHandle.writePage(pageNum,page);
					ixfileHandle.writePage(nodeDesc.prev,leftsibling);
					treeop = OP_Dist;
					dprintf("oldoffset is %d\n offset is %d\n",oldoffset,offset);
					dprintf("treeop is OP_Dist, the rightest one\n");


				}else{
					//merge, move data to left sibling

					//push down key
					FindLastKey(leftsibling,lastkeyDesc);
					memcpy(&beginkeyDesc,page,sizeof(KeyDesc));
					keyDesc.leftNode = lastkeyDesc.rightNode;
					keyDesc.rightNode = beginkeyDesc.leftNode;
					//copy push down key to lefsibling
					memcpy((char *)leftsibling+leftnodeDesc.size,&keyDesc,sizeof(keyDesc));
					leftnodeDesc.size += sizeof(keyDesc);
					memcpy((char *)leftsibling+leftnodeDesc.size,keyDesc.keyValue,keyDesc.keySize);
					leftnodeDesc.size += keyDesc.keySize;


					memcpy((char *)leftsibling+leftnodeDesc.size,(char *)page,nodeDesc.size);
					leftnodeDesc.size += nodeDesc.size;
					leftnodeDesc.next = nodeDesc.next;
					//update leftnodeDesc to leftsibling
					memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
					dprintf("Merge, the new left nodeDesc.size is %d\n",leftnodeDesc.size);
					treeop = OP_Merge;
					ixfileHandle.deletePage(pageNum);
					//write page to disk
					ixfileHandle.writePage(nodeDesc.prev,leftsibling);


					//the distance to the ending of entries doesn't change
					oldoffset = leftnodeDesc.size - (nodeDesc.size - oldoffset);
					offset = leftnodeDesc.size - (nodeDesc.size - offset);
					dprintf("oldoffset is %d\n offset is %d\n",oldoffset,offset);
					dprintf("treeop is OP_Merge, the rightest one\n");
					//update nodeDesc of new right sibling page
					assert(tempnext == nodeDesc.next);
					assert(tempprev == nodeDesc.prev);
					if( tempnext != InvalidPage ){
						//read new right sibling page
						ixfileHandle.readPage(nodeDesc.next,extrapage);
						//update nodeDesc in new right sibling
						memcpy(&extranodeDesc,(char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
						extranodeDesc.prev = tempprev;
						memcpy((char *)extrapage+PAGE_SIZE-sizeof(NodeDesc),&extranodeDesc,sizeof(NodeDesc));
						//write page to disk
						ixfileHandle.writePage(nodeDesc.next,extrapage);


					}

				}
			}
		}

	}


	//recursively call TraverseTreeInsert
	dprintf("Before entering the next level\nrightnode is %d\ncurrentpageNUm is %d\n",rightnode,currentpageNum);
	ixfileHandle.readPage(currentpageNum,nextpage);
	memcpy(&nextnodeDesc,(char *)nextpage+PAGE_SIZE-sizeof(NodeDesc),sizeof(NodeDesc));
	//dprintf("Copycat1 deleting or modifying the key\n");

	if(nextnodeDesc.type == Leaf){
		dprintf("nextnodeDesc.type == Leaf\n");
		nexttreeop = deleteFromLeaf(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, currentkeyDesc, nextRightmost);

		//assert((nexttreeop == OP_Merge || nexttreeop == OP_Dist || nexttreeop == OP_None) && "nexttreeop should be OP_Merge,OP_Dist or OP_None");

	}else if(nextnodeDesc.type == NonLeaf){
		dprintf("nextnodeDesc.type == NonLeaf\n");

		nexttreeop = TraverseTreeDelete(ixfileHandle,attribute,key,rid,nextpage,currentpageNum, currentkeyDesc, nextRightmost);

		//assert((nexttreeop == OP_Merge || nexttreeop == OP_Dist || nexttreeop == OP_None) && "nexttreeop should be OP_Merge,OP_Dist or OP_None");



	}else{
		assert("page type should be leaf or NonLeaf\n");
	}
	dprintf("Return from the next level\nrightnode is %d\ncurrentpageNUm is %d\n",rightnode,currentpageNum);

	//dprintf("Copycat2 deleting or modifying the key\n");
	checkKeyInt(ixfileHandle, attribute, page);
	dprintf("Before deleting or modifying the key\n");

	if(nexttreeop == OP_Merge ){
		dprintf("nexttreeop is OP_Merge\n");

		if(rightMost != 1){
				dprintf("rightMost != 1\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);
				//if nodeDesc.next != InvalidPage, offset and oldoffset do not change
				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update sibling KeyDesc's leftnode to  keep link-list
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor

				nodeDesc.size -= ((int)offset - (int)oldoffset);
				//assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\n",((int)offset-(int)oldoffset));


				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				assert(nodeDesc.size != 0 && "nodeDesc.size != 0");
				ixfileHandle.writePage(pageNum,page);


		}else{

			dprintf("rightMost == 1\n");

			if(treeop == OP_Dist){
				dprintf("treeop is OP_Dist\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);
				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				nodeDesc.size -= ((int)offset - (int)oldoffset);
				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\n",((int)offset-(int)oldoffset));


				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				assert(nodeDesc.size != 0 && "nodeDesc.size != 0");

				//write page to disk
				ixfileHandle.writePage(pageNum,page);
			}else if(treeop == OP_Merge){
				dprintf("treeop is OP_Merge\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);

				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)leftsibling+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)leftsibling+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)leftsibling+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)leftsibling+offset,leftnodeDesc.size-offset);
				memcpy((char *)leftsibling+oldoffset,bufferpage,leftnodeDesc.size-offset);

				//update page descriptor
				leftnodeDesc.size -= ((int)offset - (int)oldoffset);
				//assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\n",((int)offset-(int)oldoffset));


				memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
				assert(nodeDesc.size != 0 && "nodeDesc.size != 0");

				//write page to disk
				ixfileHandle.writePage(nodeDesc.prev,leftsibling);
			}else if(treeop == OP_None){
				dprintf("treeop is OP_None\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);

				//if nodeDesc.next != InvalidPage, offset and oldoffset do not change
				//fetch the deleted key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update sibling KeyDesc
				memcpy(&siblingkeyDesc,(char *)page+offset,sizeof(KeyDesc));
				siblingkeyDesc.leftNode = deletedkeyDesc.leftNode;
				memcpy((char *)page+offset,&siblingkeyDesc,sizeof(KeyDesc));
				//move data to delete the key
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				dprintf("orginal nodeDesc.size is %d\n",nodeDesc.size);
				nodeDesc.size -= ((int)offset - (int)oldoffset);
				//assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\nnodeDesc.size becomes %d\n",(offset-oldoffset),nodeDesc.size);


				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(pageNum,page);

				//for root page,
				//if the page is empty, update the directory to set the new root page
				if(nodeDesc.size == 0){
					if(IsRightest){
						//currentpageNum is rightNode
						dprintf("Empty root page\n IsRightest is 1\n");
						//update root page number
						dprintf("in TraverseTreeDelete\n newroot is %d\n",templeftNode);
						ixfileHandle.updateRootPage(templeftNode);
						//delete this empty page
						ixfileHandle.deletePage(pageNum);
						dprintf("new root page is %d\n",ixfileHandle.findRootPage());

					}else{
						//currentpageNum is leftNode
						dprintf("Empty root page\n IsRightest is 0\n");
						//update root page number
						dprintf("in TraverseTreeDelete\n newroot is %d\n",templeftNode);
						ixfileHandle.updateRootPage(templeftNode);
						//delete this empty page
						ixfileHandle.deletePage(pageNum);
						dprintf("new root page is %d\n",ixfileHandle.findRootPage());

					}
				}
			}
		}

	}else if(nexttreeop == OP_Dist){
		dprintf("nexttreeop is OP_Dist\n");

		if(rightMost != 1){
			dprintf("rightMost != 1\n");
			dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);

			//if nodeDesc.next != InvalidPage, offset and oldoffset do not change
			//fetch the to-be-modified key
			memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));
			dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

			//update to-be-modified KeyDesc.keySize
			deletedkeyDesc.keySize = currentkeyDesc.keySize;
			memcpy((char *)page+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
			oldoffset += sizeof(KeyDesc);
			//modified the key value
			memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
			memcpy((char*)page+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
			oldoffset += currentkeyDesc.keySize;
			memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

			//update page descriptor
			nodeDesc.size -= ((int)offset - (int)oldoffset);
			dprintf("nodeDesc.size is %d\n",nodeDesc.size);
			assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
			dprintf("offset - oldoffset is %d\n",(offset-oldoffset));


			memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
			//write page to disk
			ixfileHandle.writePage(pageNum,page);



		}else{
			dprintf("rightMost == 1\n");
			if(treeop == OP_Dist){
				dprintf("treeop is OP_Dist\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);

				//fetch the to-be-modified key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update to-be-modified KeyDesc.keySize
				deletedkeyDesc.keySize = currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
				oldoffset += sizeof(KeyDesc);
				//modified the key value
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char*)page+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
				oldoffset += currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				nodeDesc.size -= ((int)offset - (int)oldoffset);
				dprintf("nodeDesc.size is %d\n",nodeDesc.size);
				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\n",(offset-oldoffset));


				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(pageNum,page);
			}else if(treeop == OP_Merge){
				dprintf("treeop is OP_Merge\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);

				//fetch the to-be-modified key
				memcpy(&deletedkeyDesc,(char *)leftsibling+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update to-be-modified KeyDesc.keySize
				deletedkeyDesc.keySize = currentkeyDesc.keySize;
				memcpy((char *)leftsibling+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
				oldoffset += sizeof(KeyDesc);
				//modified the key value
				memcpy(bufferpage,(char *)leftsibling+offset,leftnodeDesc.size-offset);
				memcpy((char*)leftsibling+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
				oldoffset += currentkeyDesc.keySize;
				memcpy((char *)leftsibling+oldoffset,bufferpage,leftnodeDesc.size-offset);

				//update page descriptor
				leftnodeDesc.size -= ((int)offset - (int)oldoffset);
				dprintf("nodeDesc.size is %d\n",leftnodeDesc.size);
				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\n",(offset-oldoffset));


				memcpy((char *)leftsibling+PAGE_SIZE-sizeof(NodeDesc),&leftnodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(nodeDesc.prev,leftsibling);
			}else if(treeop == OP_None){
				dprintf("treeop is OP_None\n");
				dprintf("offset is  %d\noldoffset is %d\n",offset,oldoffset);

				//fetch the to-be-modified key
				memcpy(&deletedkeyDesc,(char *)page+oldoffset,sizeof(KeyDesc));
				dprintf("deleted key's leftNode is %d\n",deletedkeyDesc.leftNode);

				//update to-be-modified KeyDesc.keySize
				deletedkeyDesc.keySize = currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,&deletedkeyDesc,sizeof(KeyDesc));
				oldoffset += sizeof(KeyDesc);
				//modified the key value
				memcpy(bufferpage,(char *)page+offset,nodeDesc.size-offset);
				memcpy((char*)page+oldoffset,currentkeyDesc.keyValue,currentkeyDesc.keySize);
				oldoffset += currentkeyDesc.keySize;
				memcpy((char *)page+oldoffset,bufferpage,nodeDesc.size-offset);

				//update page descriptor
				nodeDesc.size -= ((int)offset - (int)oldoffset);
				dprintf("nodeDesc.size is %d\n",nodeDesc.size);
				assert((offset - oldoffset)>=0 &&"(offset - oldoffset)>=0");
				dprintf("offset - oldoffset is %d\n",(offset-oldoffset));


				memcpy((char *)page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
				//write page to disk
				ixfileHandle.writePage(pageNum,page);
			}
		}

	}



	assert( keyValue == keyDesc.keyValue && "keyValue should equal to keyDesc.keyValue ");
	checkKeyInt(ixfileHandle, attribute, page);
	//if this page is merged, check integrity from leftsibling
	if(rightMost == 1 && treeop == OP_Merge){
		checkPageInt(ixfileHandle, leftsibling, nodeDesc.prev,false);
	}else{
		checkPageInt(ixfileHandle, page, pageNum,false);
	}
	if(nexttreeop == OP_Error){
		treeop = nexttreeop;
	}
	free(deletepage);
	free(extrapage);
	free(leftsibling);
	free(rightsibling);
	free(nextpage);
	free(bufferpage);
	free(currentkeyDesc.keyValue);
	free(nextkeyDesc.keyValue);

	return treeop;

}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	// find root first
	dprintf("in deleteEntry\n");
	int Rightmost = 1;

	PageNum root = ixfileHandle.findRootPage();
	void *page = malloc(PAGE_SIZE);
	dprintf("root pageNum is %d\n",root);
	rc = ixfileHandle.readPage(root,page);

	KeyDesc keyDesc;

	keyDesc.keyValue=malloc(maxvarchar);

	// check if root needs to be split
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

	NodeType type = nodeDesc.type;
	PageSize size = nodeDesc.size;

	// traverse tree
	if(type==Leaf){
		//root page is leaf page
		dprintf("Original root page is Leaf\n");
		TreeOp treeop=deleteFromLeaf(ixfileHandle, attribute, key, rid, page, root, keyDesc, Rightmost);

		//	assert( ((treeop == OP_Dist) || (treeop == OP_Merge) || (treeop == OP_None)) && "treeop should be OP_Merge, OP_Dist or OP_None"  );
		free(keyDesc.keyValue);
		free(page);
		if( treeop == OP_Error ) return FAILURE;
		dprintf("type==Leaf\ndeleteEntry end\n");

		return 0;

	}else if(type == NonLeaf){
		//root page is NonLeaf
		dprintf("Original root page is NonLeaf\n");
		TreeOp treeop=TraverseTreeDelete(ixfileHandle, attribute, key, rid, page, root, keyDesc, Rightmost);

		free(keyDesc.keyValue);
		free(page);
		if( treeop == OP_Error ) return FAILURE;
		dprintf("type==NonLeaf\ndeleteEntry end\n");
		return 0;
	}else{
		assert("root page should be Leaf or NonLeaf");
	}
	//

	return -1;
}


TreeOp IndexManager::deleteFromLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page,

		PageNum pageNum, KeyDesc &keyDesc, int rightMost)

{

	//checkPageInt(ixfileHandle, page, pageNum);
	TreeOp operation = OP_None;
	// retrieve node info
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	int offset = 0 ;
	PageNum tempnext = nodeDesc.next;
	PageNum tempprev = nodeDesc.prev;
	// potential split page buffer
	void *nextPage = malloc(PAGE_SIZE);

	bool found = false;
	while( offset < nodeDesc.size ){
		DataEntryDesc ded;
		memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );

		ded.keyValue = malloc(ded.keySize);
		memcpy( ded.keyValue, (char*)page+offset+DataEntryKeyOffset, ded.keySize);

		// compare the key to find the deleted record
		int result = keyCompare(attribute, ded.keyValue, key);

//		printf("pageNum %u key %d result %d offset %d nodeDesc.size %d",pageNum,*(int*)key, *(int*)ded.keyValue, offset, nodeDesc.size);
//		printf(" entry size %d\n", sizeof(DataEntryDesc)+ded.keySize+ded.numOfRID*sizeof(RID) );
		// if it only contains 1 RID , remove whole entries
		if( result == 0 && ded.numOfRID == 1){
			// use nextPage as temp buffer
			dprintf("result ==0\n offset is %d\n rid.pageNum is %d\n rid.slotNum is %d\n",offset,rid.pageNum,rid.slotNum);
			int entrySize = sizeof(DataEntryDesc) + ded.keySize + sizeof(RID);
			//dprintf("entrySize is %d\n ded.keySize is %d\nnodeDesc.size is  %d\n",entrySize,ded.keySize,nodeDesc.size);
			memcpy( nextPage, (char*)page+offset+entrySize , nodeDesc.size - ( offset + entrySize ) );
			memcpy( (char*)page+offset , nextPage, nodeDesc.size - ( offset + entrySize ) );

			nodeDesc.size -= entrySize;
			memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
			found = true;
			free(ded.keyValue);		
			break;
		}
/*
		else if( result == 0 && ded.overflow != InvalidPage ){
			dprintf("result == 0 && ded.overflow != InvalidPage\n offset is %d\n
				 rid.pageNum is %d\n rid.slotNum is %d\n",offset,rid.pageNum,rid.slotNum);

			RC rc;
			rc = ixfileHandle.readPage( ded.overflow , page );
			assert( rc == SUCCESS && "Error in reading overflow page in deletion");
			memcpy( &ded, page , sizeof(DataEntryDesc) );
			for( int i=0; i< ded.numOfRID; i++){
				RID t_rid;
				memcpy( &t_rid, (char*)page+sizeof(DataEntryDesc)+ded.keySize + sizeof(RID)*i, sizeof(RID) );
				if( rid.pageNum == t_rid.pageNum && rid.slotNum == t_rid.slotNum ){
					ded.numOfRID -= 1;
					break;
				}
			}
			memcpy(page, &ded, sizeof(DataEntryDesc) );
			free(nextPage);
			free(ded.keyValue);		
			return operation;

		}
*/
		else if( result == 0 && ded.numOfRID > 1) {
			// if it has more than two RIDs, remove the one in the list
			dprintf("RID List\n offset is %d\n rid.pageNum is %d\n rid.slotNum is %d\n",offset,rid.pageNum,rid.slotNum);

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
					found = true;
					break; // break for loop
				}

			}
			if( !found && ded.overflow != InvalidPage ){

			    RC rc;
			    rc = ixfileHandle.readPage( ded.overflow , page );
			    assert( rc == SUCCESS && "Error in reading overflow page in deletion");
			    memcpy( &ded, page , sizeof(DataEntryDesc) );
			    for( int i=0; i< ded.numOfRID; i++){
				RID t_rid;
				memcpy( &t_rid, (char*)page+sizeof(DataEntryDesc)+ded.keySize + sizeof(RID)*i, sizeof(RID) );
				if( rid.pageNum == t_rid.pageNum && rid.slotNum == t_rid.slotNum ){
					ded.numOfRID -= 1;
					found = true;
					break;
				}
			    }
			    memcpy(page, &ded, sizeof(DataEntryDesc) );
			    free(nextPage);
			}

			free(ded.keyValue);		
			break; // break while loop
		}

		free(ded.keyValue);		
		offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);

	}




	if( !found ) { return OP_Error; }

	// if this page is root page, dont apply merge / redistribution.
	if( nodeDesc.size < LowerThreshold && pageNum != ixfileHandle.findRootPage() ){
		dprintf("merge / des case %d \n", *(int*)key);
		dprintf("nodeDesc.prev is %d\nnodeDesc.next is %d\n",nodeDesc.prev,nodeDesc.next);
		NodeDesc nNodeDesc; // next node ( could be previous )
		// right most leaf case
		if( nodeDesc.next == InvalidPage || rightMost == 1){
			dprintf("nodeDesc.next == InvalidPage || rightMost == 1\n");
			// read previous page since there is no next page
			ixfileHandle.readPage( nodeDesc.prev, nextPage );
			memcpy( &nNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

			// re-distribution case , else it needs to merge
			if( nodeDesc.size + nNodeDesc.size > UpperThreshold ){
				offset = 0;
				while( offset < nNodeDesc.size / 3 ){
					DataEntryDesc ded;
					memcpy( &ded, (char*)nextPage+offset, sizeof(DataEntryDesc) );
					offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
				}
				// move next page stuff to current page
				// and push current page stuff back
				void *temp = malloc(PAGE_SIZE);
				memcpy( temp, page, nodeDesc.size );
				memcpy( page, (char*)nextPage+offset, nNodeDesc.size - offset );
				memcpy( (char*)page+( nNodeDesc.size - offset), temp, nodeDesc.size );
				nodeDesc.size += ( nNodeDesc.size - offset );
				memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc));
				nNodeDesc.size = offset;
				memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &nNodeDesc, sizeof(NodeDesc));
				free(temp);


				DataEntryDesc newKeyEntry;
				memcpy( &newKeyEntry, page, sizeof(DataEntryDesc) );
				memcpy( keyDesc.keyValue, (char*)page+sizeof(DataEntryDesc), newKeyEntry.keySize);

				keyDesc.rightNode = pageNum;
				keyDesc.leftNode = nodeDesc.prev;
				keyDesc.keySize = newKeyEntry.keySize;
				ixfileHandle.writePage( pageNum, page );
				ixfileHandle.writePage( nodeDesc.prev, nextPage );
				operation = OP_Dist;
			}else{
				// merge case, nextPage is actually previous page
				memcpy( (char*)nextPage+nNodeDesc.size, page, nodeDesc.size);
				nNodeDesc.size += nodeDesc.size;
			
				// if it's not the real right most leaf, after merge and deletion
				// append next page's prev link to current page's left page
				if( nodeDesc.next != InvalidPage ){
				    void *nnPage = malloc(PAGE_SIZE);
				    ixfileHandle.readPage( nodeDesc.next, nnPage );
				    NodeDesc ntNodeDesc;
				    memcpy( &ntNodeDesc, (char*)nnPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
				    ntNodeDesc.prev = nodeDesc.prev;
				    memcpy( (char*)nnPage+PAGE_SIZE-sizeof(NodeDesc), &ntNodeDesc, sizeof(NodeDesc) );
				    ixfileHandle.writePage( nodeDesc.next, nnPage );
				    free(nnPage);
				}
				nNodeDesc.next = nodeDesc.next;
				ixfileHandle.deletePage( pageNum );
				memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &nNodeDesc, sizeof(NodeDesc));
				operation = OP_Merge;
				ixfileHandle.writePage( nodeDesc.prev , nextPage );

			}



		}else{
			dprintf("not local rightmost or global rightmost\n");
			// normal case
			ixfileHandle.readPage( nodeDesc.next, nextPage );
			memcpy( &nNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
			assert( nodeDesc.next != InvalidPage && rightMost != 1 && "WTF");
			// re-distribution case , else it needs to merge
			if( nodeDesc.size + nNodeDesc.size > UpperThreshold ){
				dprintf("nodeDesc.size + nNodeDesc.size > UpperThreshold\n");
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

				// write NodeDesc back 
				memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc));
				memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &nNodeDesc, sizeof(NodeDesc));

				DataEntryDesc newKeyEntry;
				memcpy( &newKeyEntry, nextPage, sizeof(DataEntryDesc) );
				memcpy( keyDesc.keyValue, (char*)nextPage+sizeof(DataEntryDesc), newKeyEntry.keySize);
				keyDesc.leftNode = pageNum;
				keyDesc.rightNode = nodeDesc.next;
				keyDesc.keySize = newKeyEntry.keySize;

				ixfileHandle.writePage( pageNum, page );
				ixfileHandle.writePage( nodeDesc.next, nextPage );
				operation = OP_Dist;
			}else{
				//checkPageInt(ixfileHandle, page, pageNum,true);
				dprintf("merge\n");

				// merge case
				memcpy( (char*)page+nodeDesc.size, nextPage, nNodeDesc.size );
				// update info to the next two page 
				if( nNodeDesc.next != InvalidPage ){
				    // overwrite nextPage with next 2 Page, since we've already merge it to our page
				    ixfileHandle.readPage(nNodeDesc.next, nextPage );
				    NodeDesc ntNodeDesc;
				    memcpy( &ntNodeDesc, (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc));
				//    printf("%d %d\n",ntNodeDesc.prev, nNodeDesc.next);
				    ntNodeDesc.prev = pageNum;
				    memcpy( (char*)nextPage+PAGE_SIZE-sizeof(NodeDesc), &ntNodeDesc, sizeof(NodeDesc));
				    ixfileHandle.writePage( nNodeDesc.next, nextPage );
				}

				// delete the next page
				ixfileHandle.deletePage( nodeDesc.next );

				// update the current page info
				// write page info back
				nodeDesc.size += nNodeDesc.size;
				nodeDesc.next = nNodeDesc.next;
				memcpy( (char*)page+PAGE_SIZE-sizeof(NodeDesc), &nodeDesc, sizeof(NodeDesc) );
				operation = OP_Merge;
				ixfileHandle.writePage( pageNum, page );
			}

		}

	}else{
		dprintf("extra case\n");
		ixfileHandle.writePage( pageNum, page );
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
			//printf("%d %s %s\n",sa.compare(sb),sa.c_str(),sb.c_str());
			return sa.compare(sb);
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
			printf("%d\n",size);
			assert( size >= 0 && "something wrong with getting varchar key size\n");
			//assert( size < 50 && "something wrong with getting varchar key size\n");
			return size+sizeof(int);
	}

}

// print key to screen
void IndexManager::printKey(const Attribute &attribute, const void *key)
{
	AttrType type = attribute.type;
	int size = -1;
	switch( type ){
		case TypeInt:
			printf("%d",*((int*)key));
			return;
		case TypeReal:
			printf("%f",*((float*)key));
			return;
		case TypeVarChar:
			memcpy( &size, key , sizeof(int) );
			assert( size >= 0 && "something wrong with getting varchar key size\n");
			//assert( size < 50 && "something wrong with getting varchar key size\n");
			string sa( (char*)key+4 , size );
			printf("%s",sa.c_str());
			return;
	}



}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool		lowKeyInclusive,
		bool        	highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	return ix_ScanIterator.init(ixfileHandle,attribute,lowKey,highKey,lowKeyInclusive,highKeyInclusive);
}
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute)
{
	RC rc;
	void *page = malloc(PAGE_SIZE);
	PageNum rootPage = ixfileHandle.findRootPage();
	ixfileHandle.readPage(rootPage,page);
	printBtree(ixfileHandle,attribute,page,0,rootPage);
	free(page);
}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page, int depth, PageNum nodeNum)
{
	RC rc;
	rc = ixfileHandle.readPage( nodeNum, page );
	assert( rc == SUCCESS && "Something wrong in read page in printBTree subTree");

	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	// print indent based on depth
	for(int i=0; i<depth; i++) printf("\t");

	if( nodeDesc.type == NonLeaf ){
		// vector to store all links ( PageNum of sub-tree )
		vector<PageNum> links;
		int offset = 0;
		KeyDesc keyDesc;
		DataEntryDesc ded;
		printf("{\"keys\":[");
		// print key in non-leaf first
		while( offset < nodeDesc.size ){
			if( offset > 0 ) printf(",");
			memcpy( &keyDesc, (char*)page+offset, sizeof(KeyDesc));
			keyDesc.keyValue = malloc( keyDesc.keySize );
			memcpy( keyDesc.keyValue,(char *) page+offset+sizeof(KeyDesc), keyDesc.keySize);
			// print key
			printf("\""); printKey( attribute, keyDesc.keyValue ); printf("\"");
			// add links to vector ( without last one )
			links.push_back( keyDesc.leftNode );
			free( keyDesc.keyValue);
			offset += sizeof(KeyDesc) + keyDesc.keySize;
		}
		printf("],\n");
		// add the last link to vectory
		links.push_back( keyDesc.rightNode );


		// start to traverse all children
		for(int i=0; i<depth; i++) printf("\t");
		printf("\"children\":[");
		for( int i=0; i<links.size(); i++){
			printBtree(ixfileHandle,attribute,page,depth+1,links[i]);
			if( i < links.size() - 1 ) printf(",\n");
		}
		printf("]}\n");
		return;
	}

	if( nodeDesc.type == Leaf ){
		int offset = 0;
		DataEntryDesc ded;
		printf("{\"keys\": [");
		while( offset < nodeDesc.size ){
			if( offset > 0 ) printf(",");
			memcpy( &ded, (char*)page+offset, sizeof(DataEntryDesc) );
			void *key = malloc( ded.keySize );
			memcpy( key , (char*)page+offset+sizeof(DataEntryDesc), ded.keySize);
			// print key
			printf("\""); printKey( attribute, key ); printf(":[");
			// print RIDs
			for(int i=0; i<ded.numOfRID; i++){
				RID rid;
				memcpy( &rid, (char*)page+offset+sizeof(DataEntryDesc)+ded.keySize+ i*sizeof(RID), sizeof(RID) );
				printf("(%d,%d)",rid.pageNum,rid.slotNum);
				if( i < ded.numOfRID-1 ) printf(",");
			}
			printf("]\"");
			free(key);
			offset += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID * sizeof(RID) ;
		}
		printf("]}\n");
		return;
	}

	assert( false && "It should return before this" );
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}


RC IX_ScanIterator::init(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool		lowKeyInclusive,
		bool        	highKeyInclusive)
{
	if( ixfileHandle.isReadable() == -1 ) return FAILURE;

	this->ixfileHandle = ixfileHandle;
	this->attribute = attribute;
	this->lowKey = (char*)lowKey;
	this->highKey = (char*)highKey;
	this->lowKeyInclusive = lowKeyInclusive;
	this->highKeyInclusive = highKeyInclusive;
	this->page = malloc(PAGE_SIZE);
	this->lowKeyNull = false;
	this->highKeyNull = false;
	im = IndexManager::instance();
	float INF = INFINITY/2, NINF = -INFINITY/2;

	if( lowKey == NULL ){
		this->lowKeyNull = true;
		this->lowKey = malloc(sizeof(float));
		memcpy( this->lowKey , &NINF, sizeof(float));
	}
	if( highKey == NULL ){
		this->highKeyNull = true;
		this->highKey = malloc(sizeof(float));
		memcpy( this->highKey , &INF, sizeof(float));
	}

	RC rc;
	// find root first
	PageNum root = ixfileHandle.findRootPage();
	rc = ixfileHandle.readPage(root,page);
	printf("root pageNum is %d\n",root);

	// Get Root Page Info
	NodeDesc nodeDesc;
	memcpy( &nodeDesc , (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc));


	// Start Tree Traversal if root is non-leaf
	PageNum returnPageNum = 0;
	if( nodeDesc.type == NonLeaf ){
		im->TraverseTree( ixfileHandle, attribute, this->lowKey, page, root, returnPageNum);
		assert( root != returnPageNum && "root should not be leaf in this case" );
		assert( returnPageNum >=1 && "something went wrong when traversing tree in scan ");

		rc = ixfileHandle.readPage(returnPageNum,page);
	}


	offsetToKey = 0;
	offsetToRID = 0;
	while( true ){

		DataEntryDesc ded;
		memcpy( &ded, (char*)page+offsetToKey, sizeof(DataEntryDesc));
		// retrieve key value
		void *key = malloc( ded.keySize );
		memcpy( key, (char*)page+offsetToKey+sizeof(DataEntryDesc), ded.keySize );
	//	printf("%d di %f low %f\n", im->keyCompare( attribute , key , this->lowKey ), *(float*)key, *(float*)this->lowKey );
		if( this->lowKeyNull ){
		    free(key);
		    return SUCCESS;
		}
		if( lowKeyInclusive ){

			if( im->keyCompare( attribute , key , this->lowKey ) >= 0 ){
				free(key);
				return SUCCESS;
			}
		}else{
			if( im->keyCompare( attribute , key , this->lowKey ) > 0 ){
				free(key);
				return SUCCESS;
			}
		}
		free(key);

		offsetToKey += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID);
	}


	return FAILURE;
}


RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{

	RC rc;
	// check if the offset exceeds the page size
	NodeDesc nodeDesc;
	memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );

	if( offsetToKey >= nodeDesc.size ){
		if( nodeDesc.next == InvalidPage ) return IX_EOF;
		rc = ixfileHandle.readPage( nodeDesc.next, page );
		assert( rc == SUCCESS && "something wrong in readpage in getNextEntry" );
		// Reset all offets for new pages;
		offsetToKey = 0;
		offsetToRID = 0;
		memcpy( &nodeDesc, (char*)page+PAGE_SIZE-sizeof(NodeDesc), sizeof(NodeDesc) );
	}



	DataEntryDesc ded;
	memcpy( &ded, (char*)page+offsetToKey, sizeof(DataEntryDesc) );

	// Read key and compare
	memcpy( key, (char*)page+offsetToKey+sizeof(DataEntryDesc), ded.keySize);

	int result = im->keyCompare( attribute, key , highKey );
	if( highKeyInclusive ){ 
		if( result > 0 ) return IX_EOF;
	}else{ 
		if( result >= 0 ) return IX_EOF;
	}

	// Read rid and return
	memcpy( &rid, (char*)page+offsetToKey+sizeof(DataEntryDesc)+ded.keySize+offsetToRID*sizeof(RID), sizeof(RID) );
	printf("RID %d %d %d\n",rid.pageNum,rid.slotNum, ded.numOfRID);
	
	offsetToRID++;
	//printf("offsetToRID %d \n", offsetToRID);

	if( offsetToRID == ded.numOfRID ){
		offsetToKey += sizeof(DataEntryDesc) + ded.keySize + ded.numOfRID*sizeof(RID) ;
		offsetToRID = 0;
	}


	return SUCCESS;
}

RC IX_ScanIterator::close()
{

	free(page);
	if( lowKeyNull ) free(lowKey);
	if( highKeyNull ) free(highKey);
	return SUCCESS;
}

IXFileHandle::IXFileHandle()
{
	error = -1;
}


IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::isReadable()
{
	return error;
}
RC IXFileHandle::initFilePointer(const string &fileName)
{
	error = fileHandle.initFilePointer( fileName );
	return error;
}

RC IXFileHandle::closeFilePointer()
{
	RC rc = fileHandle.closeFilePointer();
	error = -1;
	return rc;
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
	assert ( pageNum > 0 && "root can not be zero\n");
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

		NodeDesc nodeDesc;
		nodeDesc.next=InvalidPage;
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
//	if( root_debug ){ assert( findRootPage() != pageNum && "WTF, Root can not be deleted\n"); }
	this->writePageCount++;
	return fileHandle.writePage(pageNum,data);
}

RC IXFileHandle::deletePage(PageNum pageNum)
{
	PageNum dir = pageNum / IXDirectorySize;
//	printf("deletePage Num %u\n",pageNum );
//	assert( dir % IXDirectorySize != 0 && "Not valid directory index\n" );
	dir = IXDirectorySize * dir;
	assert( dir % IXDirectorySize == 0 && "Directory Page should be multiple of IXDirectorySize");
	int pageIndex = pageNum % IXDirectorySize;
	assert( pageIndex >= 1 && pageIndex < IXDirectorySize && "Not valid page index \n");
	assert( findRootPage() != pageNum && "WTF, Root can not be deleted\n");
	
	dprintf("root %u pageNum %u dir %u pageIndex %u \n",findRootPage(),pageNum, dir,pageIndex);
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage( dir, page );
	// mark the slot as empty
	PageNum empty = 0;
	memcpy( (char*)page+pageIndex*sizeof(PageNum) , &empty , sizeof(PageNum) );
	fileHandle.writePage( dir, page );
	free(page);
	return SUCCESS;
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
