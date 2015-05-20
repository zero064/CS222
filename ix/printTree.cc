#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int printTree(const string &indexFileName)
{
    // create index file
    assertCreateIndexFile(success, indexManager, indexFileName);

    // open index file
    IXFileHandle fileHandle;
    assertOpenIndexFile(success, indexManager, indexFileName, fileHandle);
    // create duplicate index file
    assertCreateIndexFile(fail, indexManager, indexFileName);

    Attribute attribute;
    attribute.length = 50;
    attribute.name = "sup";
    attribute.type = TypeVarChar;

	void *key = malloc( 4 + 1);
	char k = 'a'; int s = 1;
	RID rid;
	for( int i = 0 ; i< 26; i++){
		rid.pageNum = i;

		memcpy( key, &s, 4 );
		memcpy( (char*)key+4, &k, 1 );
		for(int j=0; j< 50; j++){
			rid.slotNum = j;
			indexManager->insertEntry(fileHandle, attribute, key, rid);
		} 

		k++;
	}
	free(key);

	printf("\n\n Print B Tree \n\n");
	indexManager->printBtree(fileHandle,attribute);
	// close index file
    assertCloseIndexFile(success, indexManager, fileHandle);



    return success;
}

int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

    const string indexFileName = "testprint";
	remove(indexFileName.c_str());
    printTree(indexFileName);
}

