#include "rbfm.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <iostream>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pagedFileManager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    getDataSize( recordDescriptor, data, true );
    return -1;
}


RC RecordBasedFileManager::createFile(const string &fileName) {
    printf("createFile\n");
    return pagedFileManager->createFile(fileName);
    //return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pagedFileManager->destroyFile(fileName);
    //return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pagedFileManager->openFile(fileName,fileHandle);
    //return -1;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pagedFileManager->closeFile(fileHandle);
    //return -1;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    void *pageData = malloc(PAGE_SIZE);
    /*
	Get directory to look for free page
    */
    void *formattedData;
    short int dataSize = writeDataToBuffer(recordDescriptor,data,formattedData); // formattedData will be malloc in this function
    rid.pageNum = findFreePage(fileHandle, dataSize );    

    /*
	Insert record into data
    */
    if( fileHandle.readPage(rid.pageNum,pageData) == FAILURE ){
	//short int size = 1;  // number of records in page, since it's new, we put our one in it
	rid.slotNum = 0;
	//memcpy( (char*)pageData+PAGE_SIZE-sizeof(short int), &size ,sizeof(short int));
	// setting first record in page , put
	RecordOffset rOffset;
	rOffset.offset = 0;
	rOffset.length = dataSize;
	memcpy( pageData, formattedData, dataSize ); 
	memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset), &rOffset, sizeof(RecordOffset) );
	// setting first page/slot descriptor 
	PageDesc pageDesc;
	pageDesc.numOfSlot = 1;
	pageDesc.recordSize = rOffset.offset + rOffset.length;
	memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageDesc) , &pageDesc, sizeof(PageDesc));
	// write it yo
	fileHandle.appendPage(pageData);

        // remember to release memory from our custom record 
	free(formattedData);
	free(pageData);
	return SUCCESS;

    }else{
	//short int index = 0;
	//memcpy(&index, (char*)pageData+PAGE_SIZE-sizeof(short int) ,sizeof(short int));
	//int offset = 0;

	// read page descriptor for slot and record information
	PageDesc pageDesc;
	memcpy( &pageDesc, (char*)pageData+PAGE_SIZE-sizeof(PageDesc) , sizeof(PageDesc) );

	// list is not continuous, read all slots from memory
	if( pageDesc.numOfSlot < 0 ){
	    short int numOfSlot = pageDesc.numOfSlot * -1 ;
	    // read all slots from memory	
	    RecordOffset *rOffset = (RecordOffset*)malloc( sizeof(RecordOffset) * numOfSlot );
	    for( int i=0; i< numOfSlot; i++){
		memcpy( &rOffset[i] , (char*)pageData+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(i+1),sizeof(RecordOffset) );
	    }
	    //memcpy(rOffset, (char*)pageData+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*numOfSlot ,sizeof(RecordOffset)*numOfSlot );
	    //printf("offset %d , length %d\n", rOffset.offset, rOffset.length); 

	    for( int i=0; i< numOfSlot; i++) {
		// found previous deleted slot to use
		
		if( rOffset[i].offset == DeletedSlotMark ){
		    rid.slotNum = i;
		    rOffset[i].offset = pageDesc.recordSize;
		    rOffset[i].length = dataSize;
		    pageDesc.recordSize += dataSize;
		    memcpy( (char*)pageData+rOffset[i].offset, formattedData, dataSize );
		    memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageDesc), &pageDesc, sizeof(PageDesc) );
		    memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(i+1), &rOffset[i], sizeof(RecordOffset) );
		    fileHandle.writePage(rid.pageNum,pageData);
		
		    // remember to release memory from our custom record 
		    free(formattedData);
		    free(rOffset);
		    free(pageData);
		    return SUCCESS;

		} 
	    }
	}

		
	// no previous deleted record found, insert a new one
	// and change the pageDesc's negative (discontinuous) flag back to positive
	if( pageDesc.numOfSlot < 0 ){
	    pageDesc.numOfSlot *= -1;
	}
	RecordOffset newSlotOffset;
	newSlotOffset.offset = pageDesc.recordSize;
	newSlotOffset.length = dataSize;
	memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(pageDesc.numOfSlot+1), &newSlotOffset, sizeof(RecordOffset) );
	memcpy( (char*)pageData+newSlotOffset.offset, formattedData, dataSize ); 

    	// update PageDesc 
	pageDesc.numOfSlot++;
	pageDesc.recordSize += dataSize;
	memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageDesc), &pageDesc , sizeof(PageDesc) );
	rid.slotNum = pageDesc.numOfSlot-1; // last index = size -1 ^.<
//	printf("wwwwwwwwww %d\n",rid.slotNum);

	// write it to file
	RC rc = fileHandle.writePage(rid.pageNum,pageData);
	assert( rc == SUCCESS && "write page should not fail ");
	// remember to release memory from our custom record 
        free(formattedData);
	free(pageData);
	return SUCCESS;
    }

    

    // remember to release memory from our custom record 
    free(formattedData);
    free(pageData);
    return FAILURE;
}


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *page = malloc(PAGE_SIZE);
    if( fileHandle.readPage(rid.pageNum,page) == FAILURE ){
	//printf("Yo readRecord failed dude (wrong page)\n");
	return FAILURE;
    }

    // read slot info 
    short int index = rid.slotNum+1;
    //printf("slot %d\n",rid.slotNum);
    RecordOffset rOffset;
    //printf("~~~~%d\n",rid.slotNum);//sizeof(RecordOffset)*index);
    memcpy( &rOffset, (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*index, sizeof(RecordOffset) );
    
    PageDesc pageDesc;
    memcpy( &pageDesc, (char*)page+PAGE_SIZE-sizeof(PageDesc), sizeof(PageDesc) );

    if( rOffset.offset == DeletedSlotMark ){
	//printf("page %d slot %d listsize %d\n",rid.pageNum,rid.slotNum,pageDesc.numOfSlot);
	//printf("Yo readRecord failed dude (deleted slot)\n");
	return FAILURE;
    }

    // we have the offset, read data from page
    void *formattedData = malloc(rOffset.length);
    memcpy( formattedData, (char*)page+rOffset.offset, rOffset.length );
    // check if the record is actually a tombstone, if it is, use tombstone to find actual data
    FieldSize fieldSize;
    memcpy( &fieldSize, formattedData, sizeof(FieldSize) );
    if( fieldSize == TombStoneMark ){
	RID trid;
	memcpy( &trid, (char*)formattedData+sizeof(FieldSize), sizeof(RID) );
	RC rc = readRecord( fileHandle, recordDescriptor, trid, data);
	free(page);
	free(formattedData);
	return rc;
    }
    //printf("offset %d length %d\n",rOffset.offset,rOffset.length);
    readDataFromBuffer(recordDescriptor,data,formattedData);
    free(page);    
    free(formattedData);

    return SUCCESS;
}


RC RecordBasedFileManager::updateFreePage(FileHandle &fileHandle,int deletedSize,int pageNum){
    int dirNum = pageNum / 512;
    
    size_t DIRECTORY_SIZE = sizeof(Directory);
    void *data = malloc(PAGE_SIZE);
    if( fileHandle.readPage(dirNum,data) == FAILURE ){
	return FAILURE;
    }

    Directory *dir = (Directory *)malloc(DIRECTORY_SIZE);
    memcpy(dir, (char*)data+(pageNum*DIRECTORY_SIZE), DIRECTORY_SIZE);
    dir->freespace += deletedSize;
    
    memcpy((char*)data+(pageNum*DIRECTORY_SIZE), dir, DIRECTORY_SIZE);
    fileHandle.writePage( dirNum , data );
    free(data);
    free(dir);
    return SUCCESS; 
}

PageNum RecordBasedFileManager::findFreePage(FileHandle &fileHandle,int recordSize)
{
    // a pointer to indicate next directory page
    int nextDir = 1, currentDir = 0;
    // size of struct directory = 8 , 4096 / 8 = 512
    size_t DIRECTORY_SIZE = sizeof(Directory);

    void *data = malloc(PAGE_SIZE);  // Directory in memeory


    // read first directory or init first directory page if it's not there
    if( fileHandle.readPage(currentDir,data) == FAILURE ){
	//printf("1st directory\n");
	// last directory used to store the next directory if this one has not enough space 
	DirectoryDesc descriptor;
	descriptor.nextDir = 1 ; 
	descriptor.size = 0;
	nextDir = descriptor.nextDir;
	memcpy( (char*)data , &descriptor , sizeof(DirectoryDesc) );

	for(int i=1; i< 512; i++){
	    Directory directory;
	    directory.pagenum = i;
	    directory.freespace = PAGE_SIZE;
	    memcpy( (char*)data + sizeof(Directory)*i , &directory , sizeof(Directory) );
	}
	
	fileHandle.appendPage(data);
    }

    // search through all linked directory page and cell inside
    while( true ){

        for(int i=1; i<512; i++){
	    void *dir = malloc(DIRECTORY_SIZE);
	    memcpy(dir, (char*)data+(i*DIRECTORY_SIZE), DIRECTORY_SIZE);
	    // check if there is free space
	    if( ((Directory *)dir)->freespace > recordSize ){
		PageNum num = ((Directory *)dir)->pagenum;
		((Directory *)dir)->freespace -= recordSize + sizeof(char) + sizeof(RecordOffset); 
		memcpy((char*)data+(i*DIRECTORY_SIZE),dir, DIRECTORY_SIZE);
		fileHandle.writePage(currentDir*512,data); // update current directory 
		free(dir);
		free(data);
//		printf("free space left %d on page %d\n",((Directory *)dir)->freespace,num);
		return num;
	    }
	    free(dir);
	} 

	currentDir++;
	// if can't find a free space from directory, create a new directory
	// last directory used to store the next directory if this one has not enough space 
	if( fileHandle.readPage(currentDir*512,data) == FAILURE ){
	    printf("creating new directory, numpage %d\n",fileHandle.getNumberOfPages());
	    DirectoryDesc descriptor;
	    nextDir++;  // point this new directory to a pseudo new directory
	    descriptor.nextDir = nextDir ; 
	    descriptor.size = 0;
	    nextDir = descriptor.nextDir;

	    memcpy( (char*)data , &descriptor , sizeof(DirectoryDesc) );

	    for(int i=1; i< 512; i++){
		Directory directory;
		directory.pagenum = i + currentDir*512;
		directory.freespace = PAGE_SIZE;
		memcpy( (char*)data + sizeof(Directory)*i , &directory , sizeof(Directory) );
	    }
	
	    fileHandle.appendPage(data);
	//    printf("numpage %d\n",fileHandle.getNumberOfPages());
	}




    }


    free(data);
}

// used to calculate the orignal data format's size from given test data
// put the flag here for options of printing 
size_t RecordBasedFileManager::getDataSize(const vector<Attribute> &recordDescriptor, const void *data, bool printFlag){
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT), offset = 0;  
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    for(int i=0; i<recordDescriptor.size(); i++){
	Attribute attribute = recordDescriptor[i];
	string name = attribute.name;
	AttrLength length = attribute.length;
	AttrType type = attribute.type;

	if(printFlag) printf("%d %s %d ",i,name.c_str(),length);
	
	if( nullFieldsIndicator[i/8] & (1 << (7-(i%8)) ) ){
	    if(printFlag) printf("null\n");
	    continue;
	}

	void *buffer;
	if( type == TypeVarChar ){
	    buffer = malloc(sizeof(int));
	    memcpy( buffer , (char*)data+offset, sizeof(int));
	    offset += sizeof(int);
	    int len = *(int*)buffer;
	    if(printFlag) printf("%i ",len);
	    free(buffer);
	    buffer = malloc(len+1);
	    memcpy( buffer, (char*)data+offset, len);
	    offset += len; 
	    ((char *)buffer)[len]='\0';
	    if(printFlag) printf("%s\n",buffer);
	    free(buffer);
	    continue; 
	}
	std::size_t size;
	if( type == TypeReal ){
	    size = sizeof(float);
	    buffer = malloc(size);
	    memcpy( buffer , (char*)data+offset, size);
	    offset += size;
	    if(printFlag) printf("%f \n",*(float*)buffer);
	} else{
	    size = sizeof(int);
	    buffer = malloc(size);
	    memcpy( buffer , (char*)data+offset, size);
	    offset += size;
	    if(printFlag) printf("%i \n",*(int*)buffer);
	}
	free(buffer);
	
    }
    if(printFlag) printf("given size %d\n",offset);
    free(nullFieldsIndicator);
    return offset;

}

// write Data to our custom compact format 
// return the actual size of data of this format in byte
// need to free formattedData after called this function
size_t RecordBasedFileManager::writeDataToBuffer(const vector<Attribute> &recordDescriptor, const void *data, void * &formattedData){
    int offset = 0;  // offset for const void data ( original data format )

    // get null indicator's size 
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    offset += nullFieldsIndicatorActualSize;
    // get field offset descriptor array
    unsigned short int *fieldOffsetDescriptor = (unsigned short int *)malloc( sizeof(unsigned short int) * recordDescriptor.size() );
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // number of field ( 2 byte )
    short int fieldSize = (short int) recordDescriptor.size();
    // get descriptor length 
    int descriptorLength = sizeof(short int) + fieldOffsetDescriptorSize;  //getDataSize( recordDescriptor,data,false) 
  

    // copy null indicator from data 
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memcpy( nullFieldsIndicator, data , nullFieldsIndicatorActualSize );

    // calculate offset from data
    for(int i=0; i<recordDescriptor.size(); i++){
	Attribute attribute = recordDescriptor[i];
	AttrType type = attribute.type;
	// if null indicator has more than 1 byte , annoying ~"~
	if( ((unsigned char*)nullFieldsIndicator)[ (0+i) / CHAR_BIT ] & (1 << (7-(i%8)) ) ){
	    printf("null\n");
	    continue;
	}

	fieldOffsetDescriptor[i] = descriptorLength + offset;
	//int formattedOffset = descriptorLength + offset - nullFieldsIndicatorActualSize;
	//printf("i %d f offset %d o offset %d \n",fieldOffset+i, formattedOffset , offset );
	//descriptor[fieldOffset+i] = (unsigned char)formattedOffset;  // assign each field's offet to descriptor
	if( type == TypeVarChar ){
	    int len;
	    memcpy( &len, (char*)data+offset, sizeof(int));
	    offset += len + sizeof(int);
	    //printf("string offset %i %i\n",len,offset);
	}else if(type == TypeReal){
	    offset += sizeof(float);
	}else if(type == TypeInt){
	    offset += sizeof(int);
	}
    }	

/*
    printf("descriptor ");
    for( int i=0; i< recordDescriptor.size(); i++){
	printf("%i ",fieldOffsetDescriptor[i]);
    }
    printf("\n");
*/
    // malloc for our custom data
    size_t oldDataSize = getDataSize( recordDescriptor,data,false);
    formattedData = malloc( descriptorLength + oldDataSize ) ; 
   
    memcpy( formattedData , &fieldSize , sizeof(short int));  // size of field
    memcpy( (char*)formattedData+sizeof(short int) ,fieldOffsetDescriptor  , fieldOffsetDescriptorSize );
    memcpy( (char*)formattedData+descriptorLength , data , oldDataSize );
 /* 
    printf("\n\ninternal test\n"); 
    void *test = malloc(1000);
    printRecord(recordDescriptor,data);
    readDataFromBuffer(recordDescriptor,test,formattedData);
    printRecord(recordDescriptor,test); 
    free(test);
*/
    free(nullFieldsIndicator);
    return ( descriptorLength +oldDataSize );
}

RC RecordBasedFileManager::readDataFromBuffer(const vector<Attribute> &recordDescriptor, void *data, const void *formattedData)
{
    int offset = 0;  // offset for const void data ( original data format )

    // get null indicator's size 
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    offset += nullFieldsIndicatorActualSize;
    // get field offset descriptor array length
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // get descriptor length 
    int descriptorLength = sizeof(FieldSize) + fieldOffsetDescriptorSize;  //getDataSize( recordDescriptor,data,false)
    // get old data's length 
    size_t oldDataSize = getDataSize( recordDescriptor,(char *)formattedData+descriptorLength,false);
    memcpy(data,(char*)formattedData+descriptorLength,oldDataSize);

   // printf("offsetToData %d %d %d\n",offsetToData, nullFieldsIndicatorActualSize, numOfField); 
    return SUCCESS;
}


size_t RecordBasedFileManager::getRecordFromPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void * &returnedData){
    void *page = malloc(PAGE_SIZE);
    if( fileHandle.readPage(rid.pageNum,page) == FAILURE ){
	//printf("Yo readRecord failed dude (wrong page)\n");
	free(page);
	return FAILURE;
    }
    // read slot info 
    short int index = rid.slotNum+1;
    RecordOffset rOffset;
    memcpy( &rOffset, (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*index, sizeof(RecordOffset) );
    
    PageDesc pageDesc;
    memcpy( &pageDesc, (char*)page+PAGE_SIZE-sizeof(PageDesc), sizeof(PageDesc) );

    if( rOffset.offset == DeletedSlotMark ){
	return FAILURE;
    }

    // we have the offset, read data from page
    returnedData = malloc(rOffset.length);
    memcpy( returnedData, (char*)page+rOffset.offset, rOffset.length );
    free(page);
    return rOffset.length;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    if( fileHandle.readPage(rid.pageNum,page) == FAILURE ){
	//printf("Yo detele an invalid record dude\n");
	return FAILURE;
    }


    // read number of slot in this page
    PageDesc pageDesc;
    memcpy( &pageDesc, (char*)page+PAGE_SIZE-sizeof(PageDesc), sizeof(PageDesc) );

    short int numOfSlot = pageDesc.numOfSlot ;
    // make sure it's positive
    if( pageDesc.numOfSlot < 0 ){
	numOfSlot *= -1;
    }

//    printf("page %d slot %d listsize %d\n",rid.pageNum,rid.slotNum,numOfSlot);

    // read whole slot descriptors 
    RecordOffset *rOffset = (RecordOffset*)malloc(sizeof(RecordOffset)*numOfSlot);
    for( int i=0; i<numOfSlot; i++){
	// i+1 is because we count the offset reversely
	memcpy( &rOffset[i], (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(i+1), sizeof(RecordOffset) );
    } 

    // check if the slot has already been marked as deleted
    RecordOffset recordToDelete = rOffset[rid.slotNum];
    if( recordToDelete.offset == DeletedSlotMark ){
	printf("Yo detele an invalid record dude\n");
	return FAILURE;
    }

    // detect tombstone
    FieldSize fieldSize;
    memcpy( &fieldSize, (char*)page+recordToDelete.offset, sizeof(FieldSize) );
    if( fieldSize == TombStoneMark ){  // tombstone found, get re-direct slot id
	RID trid;
	memcpy( &trid, (char*)page+recordToDelete.offset+sizeof(FieldSize), sizeof(RID) );
	// delete the actual record on other page
	if( deleteRecord( fileHandle, recordDescriptor, trid) == FAILURE ){
	    printf( "something wrong in deletion with tombstone\n");
	    return FAILURE;
	}
    }

    // if it's the last one, only need to update the PageDesc
    if( rid.slotNum == numOfSlot-1 && pageDesc.numOfSlot >= 0){
	pageDesc.numOfSlot -= 1;
    }else{
        int sizeToMove = 0;
        // update the rest records and slots behind it
        for( int i=0; i<numOfSlot; i++){
	    if( rOffset[i].offset > recordToDelete.offset ){
		rOffset[i].offset -= recordToDelete.length;
		sizeToMove += rOffset[i].length;
		memcpy( (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(i+1), &rOffset[i] , sizeof(RecordOffset) );
	    }
	}
    
	// pack the whole memory in page to front 
	void *temp = malloc( sizeToMove );
	memcpy( temp, (char*)page+recordToDelete.offset+recordToDelete.length, sizeToMove ); 
	memcpy( (char*)page+recordToDelete.offset, temp, sizeToMove );
	free(temp);

	// change it into negative value to indicate the slot list is not continous 
	if( pageDesc.numOfSlot > 0 ){
	    pageDesc.numOfSlot *= -1;
	}
    }

    // mark it as deleted, write it back to page
    recordToDelete.offset = DeletedSlotMark;
    memcpy( (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(rid.slotNum+1), &recordToDelete , sizeof(RecordOffset) );



    // update slot info in this page, write it back to disk
    pageDesc.recordSize -= recordToDelete.length;  
    memcpy( (char*)page+PAGE_SIZE-sizeof(PageDesc), &pageDesc, sizeof(PageDesc) );
    fileHandle.writePage(rid.pageNum,page);
    updateFreePage(fileHandle ,recordToDelete.length ,rid.pageNum);
    free(page);    

    return SUCCESS;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    // read page from disk
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage( rid.pageNum, page );
    // get original record info    
    RecordOffset rOffset;
    memcpy( &rOffset, (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(rid.slotNum+1), sizeof(RecordOffset) );
 
    // detect if the record we want to updata is a tombstone
    FieldSize fieldSize;
    memcpy( &fieldSize, (char*)page+rOffset.offset, sizeof(FieldSize) );
    if( fieldSize == TombStoneMark ){
	RID trid;
	memcpy( &trid, (char*)page+rOffset.offset+sizeof(FieldSize), sizeof(RID) );
	// delete the tombstone data on other page first
	// possibly a bug exists here, since the recordDescriptor could be updated
	// but I'm not sure my code has the knowledge to detect it.
	deleteRecord(fileHandle, recordDescriptor, trid );
    }


    void *formattedData; // new updated data
    size_t dataSize = writeDataToBuffer(recordDescriptor,data,formattedData); // formattedData will be malloc in this function
    
   // calculate space left
    PageDesc pageDesc; 
    memcpy( &pageDesc, (char*)page+PAGE_SIZE-sizeof(PageDesc), sizeof(PageDesc) ); 
    size_t freeSpaceLeft = PAGE_SIZE - ( pageDesc.recordSize + sizeof(PageDesc) + sizeof(RecordOffset) * pageDesc.numOfSlot + rOffset.length);

    int sizeDiff = 0;
    // if run out of space, do tombstone
    if( dataSize >= freeSpaceLeft ){
	// find new page to insert updated record
	RID trid;
	if( insertRecord( fileHandle, recordDescriptor, data, trid) == FAILURE ) return FAILURE;

	// write tombstone to original page 
	int tombstoneSize = sizeof(FieldSize) + sizeof(RID);
	memcpy( (char*)page+rOffset.offset, &TombStoneMark, sizeof(short int) );
	memcpy( (char*)page+rOffset.offset+sizeof(FieldSize), &trid, sizeof(RID) );

 
	// pack page
	int offsetBehindTarget = rOffset.offset + rOffset.length;
	void *temp = malloc( pageDesc.recordSize - offsetBehindTarget );
	memcpy( temp, (char*)page + offsetBehindTarget, pageDesc.recordSize - offsetBehindTarget );
	memcpy( (char*)page+rOffset.offset + tombstoneSize, temp, pageDesc.recordSize - offsetBehindTarget );
	free(temp);

	// update page info and tombstone record slot 
	pageDesc.recordSize -= ( rOffset.length - tombstoneSize );
	sizeDiff = rOffset.length - tombstoneSize;
	rOffset.length = tombstoneSize;

	memcpy( (char*)page + PAGE_SIZE - sizeof(PageDesc), &pageDesc, sizeof(PageDesc) );
	memcpy( (char*)page + PAGE_SIZE - sizeof(PageDesc)-sizeof(RecordOffset)*(rid.slotNum+1), &rOffset, sizeof(RecordOffset) );
	
    }else{
	// move records behind the data back 
	int recordsDataSize = pageDesc.recordSize - ( rOffset.offset + rOffset.length );
	void *temp = malloc( recordsDataSize );
	memcpy( temp, (char*)page + rOffset.offset + rOffset.length, recordsDataSize );
	memcpy( (char*)page + rOffset.offset, formattedData, dataSize ); // paste updated data

	memcpy( (char*)page + rOffset.offset + dataSize, temp, recordsDataSize ); // paste all the records back
	// update page info 
	if( (int)dataSize - (int)rOffset.length < 0 ) { printf("bug in updated\n"); return FAILURE; }
	pageDesc.recordSize += dataSize - rOffset.length;
	sizeDiff = dataSize - rOffset.length;
	rOffset.length = dataSize; // update the update slot size info
	
    }

    // update slots' offset which are behind the updated record
    for( int i=0; i<pageDesc.numOfSlot; i++ ){
	RecordOffset slot;
	memcpy( &slot, (char*)page + PAGE_SIZE - sizeof(PageDesc) - sizeof(RecordOffset)*(i+1), sizeof(RecordOffset) );
	if( slot.offset > rOffset.offset ){
	    slot.offset -= sizeDiff;
	    memcpy( (char*)page + PAGE_SIZE - sizeof(PageDesc) - sizeof(RecordOffset)*(i+1), &slot, sizeof(RecordOffset) );
	}	
    }
    // update the updated record's slot info 
    memcpy( (char*)page + PAGE_SIZE - sizeof(PageDesc) - sizeof(RecordOffset)*(rid.slotNum+1), &rOffset, sizeof(RecordOffset) );
    fileHandle.writePage(rid.pageNum,page);
    free(page); 
    free(formattedData);
    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{

    void *returnedData;
    size_t size = getRecordFromPage(fileHandle, recordDescriptor, rid, returnedData);
    if( size == FAILURE ) return FAILURE;
    // get field offset descriptor array length
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // get descriptor length 
    int descriptorLength = sizeof(short int) + fieldOffsetDescriptorSize;  //getDataSize( recordDescriptor,data,false)
    
    
    for( int i=0 ; i<recordDescriptor.size(); i++ ){
	// get the attribute index we want
	if( attributeName.compare( recordDescriptor[i].name ) == 0 ){
	    AttrType type = recordDescriptor[i].type;
	    int nullIndicatorOffet = ( i / CHAR_BIT );
	    char nullIndicator;
	    memcpy( &nullIndicator, (char*)returnedData+descriptorLength+nullIndicatorOffet , sizeof(char));
	    if( nullIndicator & (1 << (7-(i%8)))  ) {
		data = malloc(1);
		memcpy( data , &nullIndicator, sizeof(char) );
		free(returnedData);
		return SUCCESS;
	    }
	    FieldOffset offset = 0;
	    memcpy(&offset, (char*)returnedData + sizeof(FieldSize) + sizeof(FieldOffset) * i, sizeof(FieldOffset));
	    int len = 0 ; 
	    if( type == TypeVarChar ){
		memcpy( &len, (char*)returnedData+offset, sizeof(int));

		len += sizeof(int);
	    }else if(type == TypeReal){
		len = sizeof(float);
	    }else if(type == TypeInt){
		len = sizeof(int);
	    }
	    // copy single attribute's data from memory	    
	    memcpy( data , &nullIndicator, sizeof(char) );
	    memcpy( (char*)data+sizeof(char), (char*)returnedData+offset ,len);
	    free(returnedData);
	    return SUCCESS;

	}
    }        

    free(returnedData);
    return FAILURE;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
			        const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
    return rbfm_ScanIterator.initScanIterator(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames);
}


RC RBFM_ScanIterator::initScanIterator(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
				       const CompOp compOp, const void *value, const vector<string> &attributeNames)
{
//    assert( value != NULL && "value pointer should not be null" );
    this->rbfm = RecordBasedFileManager::instance();
    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->conditionAttribute = conditionAttribute;
    this->attributeNames = attributeNames;
    this->compOp = compOp;
    this->value = (char*)value;
    /*
    int v, v2 = 20;
    memcpy( &v, this->value, sizeof(int) );
    printf("%d\n",v); 
    printf("%d\n",memcmp( &v, &v2 ,sizeof(int)) );
    */
    // init, start from 1st record, <1,0>
    this->c_rid.pageNum = 0;
    this->c_rid.slotNum = 0;
    pageDesc.numOfSlot = -1;
    return SUCCESS;
}

RC RBFM_ScanIterator::getFormattedRecord(void *returnedData, void *data)
{
     // get field offset descriptor array length
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // get descriptor length 
    int descriptorLength = sizeof(short int) + fieldOffsetDescriptorSize;  


    unsigned int nullFieldsIndicatorActualSize = ceil((double)attributeNames.size() / CHAR_BIT );
    unsigned char *nullIndicator = (unsigned char*)malloc( nullFieldsIndicatorActualSize );
    memset( nullIndicator, 0 , nullFieldsIndicatorActualSize );

    int offset = nullFieldsIndicatorActualSize ; // offset starts from real data ( behind null indicator )
    for(int i=0; i<attributeNames.size(); i++){
	for(int j=0; j<recordDescriptor.size(); j++){
	    if( attributeNames[i].compare( recordDescriptor[j].name ) == 0 ){
		AttrType type = recordDescriptor[j].type;
		
		int o_nullIndicatorOffet = ( j / CHAR_BIT );  // null indicator position in original data
		//printf("sup j %d\n",o_nullIndicatorOffet);
		char o_nullIndicator; // null indicator in oringinal data
		// copy original data's null indicator byte set
		memcpy( &o_nullIndicator, (char*)returnedData + descriptorLength + o_nullIndicatorOffet , sizeof(char));
	    	// if it's null 
		if( o_nullIndicator & (1 << (7-(j%8)))  ){
		    nullIndicator[ i / 8 ] = ( 1 << (7 - (i%8))  ) ;
		    break;
		}

		FieldOffset f_offset = 0;
		memcpy(&f_offset, (char*)returnedData + sizeof(FieldSize) + sizeof(FieldOffset) * j, sizeof(FieldOffset));
		int t_len = 0;
		if( type == TypeVarChar ){
		    memcpy( &t_len, (char*)returnedData+f_offset, sizeof(int) );
		    char* temp = (char*)malloc(t_len);
		    memcpy(temp, (char*)returnedData+f_offset+sizeof(int),t_len);
		//    printf("\n\n%s %d %s\n",attributeNames[i].c_str(),f_offset,temp);
		    t_len += sizeof(int);
		}else if(type == TypeReal){
		    t_len = sizeof(float);
		}else if(type == TypeInt){
		    t_len = sizeof(int);
		}
		
		memcpy( (char*)data+offset, (char*)returnedData+f_offset , t_len);
		offset += t_len;
		break;
	    }
	}
    }
    memcpy( data, nullIndicator, nullFieldsIndicatorActualSize);
    free(nullIndicator);
    //free(returnedData);
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    bool found = false;
    RecordOffset rOffset;
    // get field offset descriptor array length
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // get descriptor length 
    int descriptorLength = sizeof(short int) + fieldOffsetDescriptorSize;  
    void *returnedData = malloc(1000);


    while( !found ){
	rid = c_rid;
    	// finish reading all records in a page
	if( (int)c_rid.slotNum+1 > (int)pageDesc.numOfSlot){
	    c_rid.slotNum = 0;
	    c_rid.pageNum++;
	    if( c_rid.pageNum % 512 == 0 ) c_rid.pageNum++;

	    if( fileHandle.readPage( c_rid.pageNum, page ) == FAILURE) return RBFM_EOF;
	    memcpy( &pageDesc, (char*)page+PAGE_SIZE-sizeof(PageDesc), sizeof(PageDesc) );
	    if( pageDesc.numOfSlot < 0 ) pageDesc.numOfSlot*=-1; // if it's an inconsistent page
	} 

	
	// read slot
	memcpy( &rOffset, (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(c_rid.slotNum+1), sizeof(RecordOffset) );
//	printf("rOffset %d %d\n",rOffset.offset,rOffset.length);
	if( rOffset.offset == DeletedSlotMark ){
	    c_rid.slotNum++;
	    continue;
	}

	// tombstone case
	FieldSize fieldSize;
	memcpy( &fieldSize, (char*)page+rOffset.offset, sizeof(FieldSize) );
	if( fieldSize == TombStoneMark ){
	    RID trid;
	    memcpy( &trid, (char*)page+rOffset.offset+sizeof(FieldSize), sizeof(RID) );
	    void *t_page = malloc(PAGE_SIZE);
	    fileHandle.readPage(trid.pageNum,t_page);
	    memcpy( &rOffset, (char*)t_page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(trid.slotNum+1), sizeof(RecordOffset) );
	    memcpy( returnedData, (char*)t_page+rOffset.offset, rOffset.length);
	    free(t_page); 
	}else{
	    memcpy( returnedData, (char*)page+rOffset.offset, rOffset.length );
	}

	if( compOp == NO_OP ){
	     getFormattedRecord(returnedData,data);
	     found = true;
	     c_rid.slotNum++;
	     break;
	}




	for( int i=0 ; i<recordDescriptor.size(); i++ ){
	    // get the condtional attribute index 
	    if( conditionAttribute.compare( recordDescriptor[i].name ) == 0 || compOp == NO_OP ){
		AttrType type = recordDescriptor[i].type;
		int nullIndicatorOffet = ( i / CHAR_BIT );
		char nullIndicator;
		memcpy( &nullIndicator, (char*)returnedData+descriptorLength+nullIndicatorOffet , sizeof(char));
		

		// if it's null & it's not NO_OP
		if( nullIndicator & (1 << (7-(i%8))) && compOp != NO_OP) {
		    break;
		}
		FieldOffset offset = 0;
		memcpy(&offset, (char*)returnedData + sizeof(FieldSize) + sizeof(FieldOffset) * i, sizeof(FieldOffset));
		int t_len = 0;
		int t_len2 = 0;
		int cmpValue = 0; 
		if( type == TypeVarChar ){
		    memcpy( &t_len, (char*)value, sizeof(int) );
		    memcpy( &t_len2, (char*)returnedData+offset, sizeof(int) );
		    if( t_len == t_len2 ){
			//printf("t_len is %d\n",t_len);
			cmpValue = memcmp( (char*)value+sizeof(int), (char*)returnedData+offset+sizeof(int), t_len);
			//printf("compare %s cmpValue %d\n",conditionAttribute.c_str(),cmpValue);
			//printf("target string %s\n",(char*)value+sizeof(int));
			//printf("string from data %s\n\n",(char*)returnedData+offset+sizeof(int));
		    }else{
			cmpValue = -1;
		    }
		    //printf("string from data %s\n\n",str);
//		    assert( cmpValue == 0 );

		}else if(type == TypeReal){
		    t_len = sizeof(float);
		    float a;
		    float f_cmpValue;
		    memcpy( &a, (char*)returnedData+offset, t_len);
		    memcpy( &f_cmpValue, value, sizeof(float));
		    f_cmpValue = a - f_cmpValue;
		    cmpValue = f_cmpValue*10000;  // convert to int, times 10000 to increase precision
		}else if(type == TypeInt){
		    t_len = sizeof(int);
		    int a;
		    memcpy( &a, (char*)returnedData+offset, t_len);
		    int b;
		    memcpy( &b, value, t_len);
		    cmpValue = a - b;
//		    printf("data %d target %d cmpValue %d\n",a,b,cmpValue);
		}
		
		//cmpValue = memcmp( (char*)returnedData+offset, value, t_len);
		switch( compOp ){
		    case NO_OP:
			// no op, call function directly
			getFormattedRecord(returnedData,data);
			found=true;
			break;
		    case EQ_OP:

			if( cmpValue == 0 ){ // function call
			    getFormattedRecord(returnedData,data);
			    found=true;
			}
			break;
		    case LT_OP:
			if( cmpValue < 0 ){
			    getFormattedRecord(returnedData,data);
			    found=true;
			}
			break;
		    case GT_OP:
			if( cmpValue > 0 ){
			    getFormattedRecord(returnedData,data);
			    found=true;
			}
			break;
		    case LE_OP:
			if( cmpValue <= 0 ){
			    getFormattedRecord(returnedData,data);
			    found=true;
			}
			break;	
		    case GE_OP:
			if( cmpValue >= 0 ){
			    getFormattedRecord(returnedData,data);
			    found=true;
			}
			break;	
		    case NE_OP:
			if( cmpValue != 0 ){
			    getFormattedRecord(returnedData,data);
			    found=true;
			}
			break;
		}

		break; // break for loop

	    }
	} // for loop 
	
	c_rid.slotNum++;
    }
    free(returnedData);
    return SUCCESS;
}
