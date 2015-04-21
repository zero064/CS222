
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

/*
	// put new record, increase and update index
	index++;  rid.slotNum = index-1;
	//printf("slot %d\n",rid.slotNum);
	// new offset 
	rOffset.offset = rOffset.offset + rOffset.length;
	rOffset.length = dataSize;
	// put new record
	memcpy( (char*)pageData+rOffset.offset, formattedData, dataSize ); 
	// update index
	memcpy( (char*)pageData+PAGE_SIZE-sizeof(short int), &index ,sizeof(short int));
	// add new record index at bottom
	memcpy( (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(RecordOffset)*index, &rOffset ,sizeof(RecordOffset));
*/

	// remember to release memory from our custom record 
        free(formattedData);
	free(pageData);
	// write it to file
	fileHandle.writePage(rid.pageNum,pageData);
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

    // if can't find a free space from directory, create a new directory
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
	    buffer = malloc(len);
	    memcpy( buffer, (char*)data+offset, len);
	    offset += len; 
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
    int offset = 0, roughOffset = 0;  // offset for const void data ( original data format )

    // get null indicator's size 
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    offset += nullFieldsIndicatorActualSize;
    // get field offset descriptor array
    unsigned short int *fieldOffsetDescriptor = (unsigned short int *)malloc( sizeof(unsigned short int) * recordDescriptor.size() );
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // number of field (1 byte )
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
    int offset = 0, roughOffset = 0;  // offset for const void data ( original data format )

    // get null indicator's size 
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    offset += nullFieldsIndicatorActualSize;
    // get field offset descriptor array length
    int fieldOffsetDescriptorSize = sizeof(unsigned short int) * recordDescriptor.size();
    // number of field (2 byte )
    short int fieldSize = (short int) recordDescriptor.size();
    // get descriptor length 
    int descriptorLength = sizeof(short int) + fieldOffsetDescriptorSize;  //getDataSize( recordDescriptor,data,false)
    // get old data's length 
    size_t oldDataSize = getDataSize( recordDescriptor,(char *)formattedData+descriptorLength,false);
    memcpy(data,(char*)formattedData+descriptorLength,oldDataSize);

/*

    // get number of fields
    char numOfField;
    memcpy( &numOfField , formattedData , sizeof(char) );

    // size(char) + nullIndicator(char*(size/8)) + offset(size*(char))
    // restore the data format of test case
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memcpy( data, (char*)formattedData+sizeof(char), nullFieldsIndicatorActualSize);


    int offsetToData = sizeof(char) + nullFieldsIndicatorActualSize + numOfField*sizeof(char) ; 
    // get estimated data size from descriptor
    int estimatedSize = 0; char offsetOfLastField = 0 , actualDataSize = 0;
//    printf("%d ",);
    void *descriptor = malloc( offsetToData );
    memcpy( descriptor , formattedData , offsetToData );
    for(int i=0;i<offsetToData; i++){
	printf("%d ",((unsigned char*)descriptor)[i]);
    }    

    memcpy( &offsetOfLastField , (char*)formattedData+offsetToData-sizeof(unsigned char),sizeof(unsigned char) );
    printf("actualDataSize %d data length %d \n",(size_t)offsetOfLastField,actualDataSize-offsetToData);
    if( recordDescriptor.back().type == TypeVarChar ){
	int len;
	memcpy( &len, (char*)formattedData+offsetOfLastField,sizeof(int));
	actualDataSize = offsetOfLastField + len + sizeof(int) ;
    }else if( recordDescriptor.back().type == TypeInt ){
	actualDataSize = offsetOfLastField + sizeof(int);
    }else if( recordDescriptor.back().type == TypeReal ){
	actualDataSize = offsetOfLastField + sizeof(float);
    }
//    memcpy( (char*)data+nullFieldsIndicatorActualSize , (char*)formattedData+offsetToData, estimatedSize); 
    
    memcpy( (char*)data+nullFieldsIndicatorActualSize , (unsigned char*)formattedData+offsetToData, actualDataSize - offsetToData );  
   */
   // printf("offsetToData %d %d %d\n",offsetToData, nullFieldsIndicatorActualSize, numOfField); 
    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    if( fileHandle.readPage(rid.pageNum,page) == FAILURE ){
//	printf("Yo detele an invalid record dude\n");
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

    // if it's the last one, only need to update the PageDesc
    if( rid.slotNum == numOfSlot-1 && pageDesc.numOfSlot >= 0){
	pageDesc.numOfSlot -= 1;
    }else{
        int sizeToMove = 0;
        // update the rest records and slots behind it
	//for( int i=rid.slotNum+1; i<numOfSlot; i++){
        for( int i=0; i<numOfSlot; i++){
	if( rOffset[i].offset > recordToDelete.offset ){
	    rOffset[i].offset -= recordToDelete.length;
	    sizeToMove += rOffset[i].length;
	    memcpy( (char*)page+PAGE_SIZE-sizeof(PageDesc)-sizeof(RecordOffset)*(i+1), &rOffset[i] , sizeof(RecordOffset) );
	}
/*
	void *temp = malloc( rOffset[i].length );
	memcpy( temp , (char*) page+rOffset[i].offset ,rOffset[i].length );

	// update the descriptor of slot
	rOffset[i].offset -= recordToDelete.length;

	// pack the actual data 
	memcpy( (char*)page+rOffset[i].offset, temp , rOffset[i].length );
	free(temp);
*/
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


}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{


}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
			        const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{



}
