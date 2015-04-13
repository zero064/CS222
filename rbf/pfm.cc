#include "pfm.h"

// own implementation 
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

// singleton
PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

// constructor
PagedFileManager::PagedFileManager()
{
}

// destructor
PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    // if file doesn't exist
    if ( access( fileName.c_str(), F_OK ) == -1  ){
	//printf("file doen't exsit \n");
	FILE* file = fopen(fileName.c_str(),"w+b");
	if( file != NULL )
	   fclose(file);
	else 
	   return FAILURE;

	return SUCCESS;
    }

    return FAILURE;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if( remove( fileName.c_str() ) != 0 ) 
	return FAILURE;
    else
	return SUCCESS;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{

    // file does exist, open it 
    if ( access( fileName.c_str(), F_OK ) == 0  ){
	//printf("file does exist, open it \n");
	if( fileHandle.initFilePointer( fileName ) == FAILURE )
	    return FAILURE; 
    	
	/*
	if( filePointer != NULL ){
	    fclose(filePointer);
	}
	filePointer = fopen(fileName.c_str(),"a+");
	printf("YOLO\n");
	// undefined area where reading header from file and write it to filaHandle obejct
	// .......
	*/
	fileHandle.readPageCounter = 0;
	fileHandle.writePageCounter = 0;
	fileHandle.appendPageCounter = 0;

	return SUCCESS;
    }


    return FAILURE;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.closeFilePointer();
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	numberOfPages = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
//    printf("r%d\n",pageNum );
    fseek( filePointer , pageNum * PAGE_SIZE ,SEEK_SET );
    int result = fread(data, sizeof(char), PAGE_SIZE, filePointer);
    if( result != PAGE_SIZE ){
	//printf("page hasn't been opened yet\n");
	return FAILURE;
    }

    readPageCounter++;
    return SUCCESS;
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    //printf("w%d\n",pageNum * PAGE_SIZE );
    if( pageNum > numberOfPages ){
	return FAILURE;
    }
    fseek( filePointer , pageNum * PAGE_SIZE , SEEK_SET );
    int result = fwrite(data, sizeof(char), PAGE_SIZE, filePointer);
  
    if( result != PAGE_SIZE ){
	printf("page hasn't been opened yet\n");
	return FAILURE;
    }
    writePageCounter++;
    return SUCCESS;
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    // append page
    fseek(filePointer, 0, SEEK_END); 
    long int fileSize = ftell(filePointer);
    int result = fwrite(data,sizeof(char),PAGE_SIZE,filePointer);
    if( result != PAGE_SIZE )
	return FAILURE;
    appendPageCounter++;
    numberOfPages++;
    return SUCCESS;
    //return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    return appendPageCounter;
//    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return SUCCESS;
    //return -1;
}

RC FileHandle::initFilePointer(const string &fileName)
{
    filePointer = fopen(fileName.c_str(),"r+b");
    if( filePointer == NULL )
	return FAILURE;
    return SUCCESS;
}

RC FileHandle::closeFilePointer()
{
  if( fclose( filePointer ) == 0 )
    return SUCCESS;
  return FAILURE;
}
