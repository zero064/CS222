
#include "rm.h"
#define DEBUG 1
#define DEBUG1 1

RC RelationManager::PrepareCatalogDescriptor(string tablename,vector<Attribute> &attributes){
	string tables="Tables";
	string columns="Columns";
	Attribute attr;

	if(tables.compare(tablename)==0){
		attr.name="table-id";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=1;
		attributes.push_back(attr);

		attr.name="table-name";
		attr.type=TypeVarChar;
		attr.length=50;
		attr.position=2;
		attributes.push_back(attr);

		attr.name="file-name";
		attr.type=TypeVarChar;
		attr.length=50;
		attr.position=3;
		attributes.push_back(attr);

		attr.name="SystemTable";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=4;
		attributes.push_back(attr);


		return 0;
	}
	else if(columns.compare(tablename)==0){
		attr.name="table-id";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=1;
		attributes.push_back(attr);

		attr.name="column-name";
		attr.type=TypeVarChar;
		attr.length=50;
		attr.position=2;
		attributes.push_back(attr);

		attr.name="column-type";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=3;
		attributes.push_back(attr);

		attr.name="column-length";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=4;
		attributes.push_back(attr);

		attr.name="column-position";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=5;
		attributes.push_back(attr);

		attr.name="NullFlag";
		attr.type=TypeInt;
		attr.length=4;
		attr.position=6;
		attributes.push_back(attr);


		return 0;
	}
	else{
		cout<<"Error! PrepareCatalogDescriptor can only be used to get Catalog's record descriptor" <<endl;
		return -1;
	}

}

RC RelationManager::CreateTablesRecord(void *data,int tableid,const string tablename,int systemtable){
	int offset=0;

	//int size=tablename.size();
	int size = tablename.size();
	//assert ( cp == size && "yo");

	char nullind=0;

	//copy null indicator
	memcpy((char *)data+offset,&nullind,1);
	offset=offset+1;

	memcpy((char *)data+offset,&tableid,sizeof(int));
	offset=offset+sizeof(int);
	//copy table name
	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tablename.c_str(),size);
	offset=offset+size;

	//copy file name
	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tablename.c_str(),size);
	offset=offset+size;

	//copyt SystemTable
	memcpy((char *)data+offset,&systemtable,sizeof(int));
	offset=offset+sizeof(int);
	#ifdef DEBUG
		cout<<endl<<"create table record "<<"offset is "<<offset<<endl;
	#endif
	return 0;

}

RC RelationManager::CreateColumnsRecord(void * data,int tableid, Attribute attr, int position, int nullflag){
	int offset=0;
	int size=attr.name.size();
	char null[1];
	null[0]=0;


	//null indicator
	memcpy((char *)data+offset,null,1);
	offset+=1;

	memcpy((char *)data+offset,&tableid,sizeof(int));
	offset=offset+sizeof(int);

	//copy VarChar
	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);
	memcpy((char *)data+offset,attr.name.c_str(),size);
	offset=offset+size;

	//copy  type
	memcpy((char *)data+offset,&(attr.type),sizeof(int));
	offset=offset+sizeof(int);

	//copy attribute length
	memcpy((char *)data+offset,&(attr.length),sizeof(int));
	offset=offset+sizeof(int);

	//copy position
	memcpy((char *)data+offset,&position,sizeof(int));
	offset=offset+sizeof(int);

	//copy nullflag
	memcpy((char *)data+offset,&nullflag,sizeof(int));
	offset=offset+sizeof(int);
	#ifdef DEBUG
		cout<<endl<<"create column record "<<"offset is "<<offset<<endl;
	#endif
	return 0;

}
RC RelationManager::UpdateColumns(int tableid,vector<Attribute> attributes){
	int size=attributes.size();
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle table_filehandle;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<Attribute> columndescriptor;
	RID rid;

	PrepareCatalogDescriptor("Columns",columndescriptor);
	if(rbfm->openFile("Columns", table_filehandle)==0){
		for(int i=0;i<size;i++){
			CreateColumnsRecord(data,tableid,attributes[i],attributes[i].position,0);
			rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
			#ifdef DEBUG2
				cout<<"In UpdateColumns"<<endl;
				rbfm->printRecord(columndescriptor,data);
			#endif
			//printf("\ncolumn RID %d,%d\n",rid.pageNum,rid.slotNum);
			//rbfm->printRecord(columndescriptor,data);
		}
		rbfm->closeFile(table_filehandle);
		free(data);
		return 0;
	}
	#ifdef DEBUG
		cout<<"There is bug on UpdateColumns"<<endl;
	#endif
	return -1;
}

int RelationManager::GetFreeTableid(){

	RM_ScanIterator rm_ScanIterator;
	RID rid;
	char *data=(char *)malloc(PAGE_SIZE);

	vector<string> attrname;
	attrname.push_back("table-id");
	int tableID;
	int foundID;
	bool scanID[TABLE_SIZE];
	std::fill_n(scanID,TABLE_SIZE,0);

	void *v = malloc(1);
	if( scan("Tables","",NO_OP,v,attrname,rm_ScanIterator)==0 ){

		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//!!!! skip null indicator
			#ifdef DEBUG1
				cout<<"before 	memcpy(&foundID,(char *)data+1,sizeof(int));"<<endl;
			#endif
			memcpy(&foundID,(char *)data+1,sizeof(int));
			#ifdef DEBUG1
				cout<<"foundID is "<<foundID<<endl;
			#endif
			scanID[foundID-1]=true;

		}
		for(int i=0;i<TABLE_SIZE;i++){
			if(!scanID[i]){
				tableID=i+1;
				break;
			}
		}

		free(data);
		rm_ScanIterator.close();
		#ifdef DEBUG
			cout<<"GET free table id: "<<tableID<<endl;
		#endif
		free(v);
		return tableID;
	}


	#ifdef DEBUG
		cout<<"There is bug on GetFreeTableid"<<endl;
	#endif
	return -1;

}
RC RelationManager::CreateVarChar(void *data,const string &str){
	int size=str.size();
	int offset=0;
	memcpy((char *)data+offset,&size,sizeof(int));
	offset+=sizeof(int);
	memcpy((char *)data+offset,str.c_str(),size);
	offset+=size;


	return 0;
}

int RelationManager::VarCharToString(void *data,string &str){
	int size;
	int offset=0;
	char * VarCharData=(char *) malloc(PAGE_SIZE);

	memcpy(&size,(char *)data+offset,sizeof(int));
	offset+=sizeof(int);

	memcpy(VarCharData,(char *)data+offset,size);
	offset+=size;

	VarCharData[size]='\0';
	string tempstring(VarCharData);
	str=tempstring;


	free(VarCharData);

	return 0;


}

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	vector<Attribute> tablesdescriptor;
	vector<Attribute> columnsdescriptor;

	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle table_filehandle;
	RID rid;


	//creat Tables
	if((rbfm->createFile("Tables"))==0){

		void *data=malloc(PAGE_SIZE);
		int tableid=1;
		int systemtable=1;

		rbfm->openFile("Tables",table_filehandle);

		PrepareCatalogDescriptor("Tables",tablesdescriptor);
		CreateTablesRecord(data,tableid,"Tables",systemtable);
		RC rc = rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);
		assert( rc == 0 && "insert table should not fail");
		//rbfm->readRecord(table_filehandle,tablesdescriptor,rid,data);
		//rbfm->printRecord(tablesdescriptor,data);		
		
		tableid=2;
		CreateTablesRecord(data,tableid,"Columns",systemtable);
		rc = rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);
		assert( rc == 0 && "insert table should not fail");
		//rbfm->readRecord(table_filehandle,tablesdescriptor,rid,data);
		//rbfm->printRecord(tablesdescriptor,data);		
		rbfm->closeFile(table_filehandle);
		//create Columns
		if((rbfm->createFile("Columns"))==0){

			UpdateColumns(1,tablesdescriptor);

			PrepareCatalogDescriptor("Columns",columnsdescriptor);
			UpdateColumns(tableid,columnsdescriptor);



			free(data);
			#ifdef DEBUG
				cout<<"successfully create catalog"<<endl;
			#endif
			return 0;
		}
	}
	#ifdef DEBUG
		cout<<"Fail to create catalog"<<endl;
	#endif
    return -1;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();

	if(rbfm->destroyFile("Tables")==0){
		if(rbfm->destroyFile("Columns")==0){
			#ifdef DEBUG
				cout<<"successfully delete Tables and Columns "<<endl;
			#endif
			return 0;
		}
	}
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	FileHandle nullhandle;
	vector<Attribute> tablesdescriptor;
	char *data=(char *)malloc(PAGE_SIZE);
	RID rid;
	int tableid;
	vector<Attribute> tempattrs=attrs;
	for(int i=0;i< tempattrs.size();i++){
		tempattrs[i].position=i+1;
	}
	if(rbfm->createFile(tableName)==0){


		if(rbfm->openFile("Tables",filehandle)==0){
			#ifdef DEBUG1
				cout<<"before get free table id "<<endl;
			#endif
			tableid=GetFreeTableid();
			#ifdef DEBUG1
				cout<<"table id is"<<tableid<<endl;
			#endif
			PrepareCatalogDescriptor("Tables",tablesdescriptor);
			CreateTablesRecord(data,tableid,tableName,0);
			RC rc = rbfm->insertRecord(filehandle,tablesdescriptor,data,rid);
			assert( rc == 0 && "insert table should not fail");
			printf("ccccccc tables rid %d,%d\n",rid.pageNum,rid.slotNum);
			#ifdef DEBUG
				cout<<"In createTable"<<endl;
				rbfm->printRecord(tablesdescriptor,data);
			#endif
			rbfm->closeFile(filehandle);
			if(UpdateColumns(tableid,tempattrs)==0){
				free(data);
				return 0;
			}
		}

	}
	#ifdef DEBUG
		cout<<"There is bug on createTable "<<endl;
	#endif
    return -1;
}

int RelationManager::getTableId(const string &tableName){

	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tableid = -1;
	char *VarChardata=(char *)malloc(PAGE_SIZE);
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");
	int count=0;

	CreateVarChar(VarChardata,tableName);
	#ifdef DEBUG
		cout<<"In getTableId tableName: "<<tableName<<endl;
	#endif

	if( scan("Tables","table-name",EQ_OP,VarChardata,attrname,rm_ScanIterator) == 0 ){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){

			//!!!! skip null indicator
			memcpy(&tableid,(char *)data+1,sizeof(int));
			printf("yoooooooooooo table id %d\n",tableid);
			count++;
			if(count>=2){
				cout<<"There are two record in Tables with same table name "<<endl;
			}
		}
		rm_ScanIterator.close();

		free(VarChardata);
		free(data);
		if( tableid == -1 ) return -1;
		return tableid;
	}
	return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	RM_ScanIterator rm_ScanIterator;
	RM_ScanIterator rm_ScanIterator2;
	RID rid;
	int tableid;

	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");

	vector<Attribute> tablesdescriptor;
	PrepareCatalogDescriptor("Tables",tablesdescriptor);
	vector<Attribute> columnsdescriptor;
	PrepareCatalogDescriptor("Columns",columnsdescriptor);

	if(rbfm->destroyFile(tableName)==0){

		tableid=getTableId(tableName);
		printf("\n\nDelete table id %d\n",tableid);
		rbfm->openFile("Tables",filehandle);
		if(RelationManager::scan("Tables","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator)==0){
			while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
				rbfm->deleteRecord(filehandle,tablesdescriptor,rid);
			}
			rbfm->closeFile(filehandle);
			rm_ScanIterator.close();

			rbfm->openFile("Columns",filehandle);
			if(RelationManager::scan("Columns","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator2)==0){
				while(rm_ScanIterator2.getNextTuple(rid,data)!=RM_EOF){
					rbfm->deleteRecord(filehandle,columnsdescriptor,rid);
				}
				rm_ScanIterator2.close();
				rbfm->closeFile(filehandle);

				free(data);
				#ifdef DEBUG
					cout<<"Successfully delete "<<tableName<<endl;
				#endif
				return 0;

			}


		}

	}
	#ifdef DEBUG
		cout<<"There is bug on deleteTable "<<endl;
	#endif
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tableid;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	attrname.push_back("NullFlag");
	Attribute attr;
	string tempstr;
	int nullflag;
	int offset = 0;



	tableid=getTableId(tableName);
	if( tableid == -1 ) return -1;
	printf("tableid ========= %d\n",tableid);
	if( scan("Columns","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator) == 0 ){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			printf("scan through attributes\n");
			//skip null indicatior
			offset=1;
			VarCharToString(data+offset,tempstr);
			attr.name=tempstr;
			offset+=(sizeof(int)+tempstr.size());
			memcpy(&(attr.type),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(attr.length),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(attr.position),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(nullflag),data+offset,sizeof(int));
			offset+=sizeof(int);
			if(nullflag==1){
				attr.length=-1;
			}
			attrs.push_back(attr);

		}
		rm_ScanIterator.close();
		free(data);
		#ifdef DEBUG
			cout<<"Successfully getAttribute "<<endl
			cout<<"Size of attrs "<< attrs.size()<<endl;
			cout<<"the first attriubt name: "<<attrs[0].name<<endl;
			cout<<"the last attriubt name: "<<attrs[attrs.size()-1].name<<endl;
		#endif
		return 0;
	}
	#ifdef DEBUG
		cout<<"There is bug on getAttribute "<<endl;
	#endif
	return -1;

}
int RelationManager::IsSystemTable(const string &tableName){
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int systemtable;
	char *VarChardata=(char *)malloc(PAGE_SIZE);
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("SystemTable");
	int count=0;

	CreateVarChar(VarChardata,tableName);

	if( scan("Tables","table-name",EQ_OP,VarChardata,attrname,rm_ScanIterator) == 0 ){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//!!!! skip null indicator
			memcpy(&systemtable,(char *)data+1,sizeof(int));
			count++;
			if(count>=2){
				cout<<"There are two record in Tables with same table name "<<endl;
			}
		}
		rm_ScanIterator.close();
		free(VarChardata);
		free(data);
		return systemtable;

	}
	return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;



	if(IsSystemTable(tableName)==0){
		RelationManager::getAttributes(tableName,descriptor);
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->insertRecord(filehandle,descriptor,data,rid)==0){

				#ifdef DEBUG
					cout<<"Successfully intsert tuple "<<endl;
				#endif
				return 0;
			}
		}


    }

    #ifdef DEBUG
		cout<<"There is bug on insertTuple "<<endl;
	#endif
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;



	if(IsSystemTable(tableName)==0){
		RelationManager::getAttributes(tableName,descriptor);
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->deleteRecord(filehandle,descriptor,rid)==0){

				#ifdef DEBUG
					cout<<"Successfully delete tuple "<<endl;
				#endif
				return 0;
			}
		}


	 }

	#ifdef DEBUG
		cout<<"There is bug on delete Tuple "<<endl;
	#endif
	return -1;

}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;



	if(IsSystemTable(tableName)==0){
		RelationManager::getAttributes(tableName,descriptor);
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->updateRecord(filehandle,descriptor,data,rid)==0){

				#ifdef DEBUG
					cout<<"Successfully update tuple "<<endl;
				#endif
				return 0;
			}
		}


	 }

	#ifdef DEBUG
		cout<<"There is bug on update Tuple "<<endl;
	#endif
	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;



		RelationManager::getAttributes(tableName,descriptor);
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->readRecord(filehandle,descriptor,rid,data)==0){

				#ifdef DEBUG
					cout<<"Successfully read tuple "<<endl;
				#endif
				return 0;
			}
		}



	#ifdef DEBUG
		cout<<"There is bug on read Tuple "<<endl;
	#endif
	return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();


	if(rbfm->printRecord(attrs,data)==0){

		#ifdef DEBUG
			cout<<"Successfully print tuple "<<endl;
		#endif
		return 0;
	}



	#ifdef DEBUG
		cout<<"There is bug on print Tuple "<<endl;
	#endif
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;



		RelationManager::getAttributes(tableName,descriptor);
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->readAttribute(filehandle,descriptor,rid,attributeName,data)==0){

				#ifdef DEBUG
					cout<<"Successfully read attribute "<<endl;
				#endif
				return 0;
			}
		}



	#ifdef DEBUG
		cout<<"There is bug on read attribute "<<endl;
	#endif
	return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;
	if(tableName.compare("Tables")==0){

		PrepareCatalogDescriptor("Tables",descriptor);
		#ifdef DEBUG1
			cout<<"In scan	prepareCatalogDescriptor(Tables,descriptor);"<<endl<<descriptor[0].name<<endl<<(descriptor.back()).name<<endl;
		#endif
	}
	else if(tableName.compare("Columns")==0){
		PrepareCatalogDescriptor("Columns",descriptor);
		#ifdef DEBUG1
			cout<<"In scan	PrepareCatalogDescriptor(Columns,descriptor);"<<endl<<descriptor[0].name<<endl<<(descriptor.back()).name<<endl;
		#endif

	}
	else{
		RelationManager::getAttributes(tableName,descriptor);
	}
	if(rbfm->openFile(tableName,filehandle)==0){
		#ifdef DEBUG1
			cout<<"In scan	before rbfm->scan"<<endl;
		#endif
		if(rbfm->scan(filehandle,descriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_ScanIterator)==0){

			#ifdef DEBUG
				cout<<"Successfully doing RelationManager scan "<<endl;
			#endif
			return 0;
		}

	}



	#ifdef DEBUG
		cout<<"There is bug on doing RelationManager scan "<<endl;
	#endif
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	vector<Attribute> descriptor;
	int position;
	int tableid;
	vector<Attribute> tempattr;
	tempattr.push_back(attr);


	tableid=getTableId(tableName);
	RelationManager::getAttributes(tableName,descriptor);
	position=(descriptor.back()).position +1;
	tempattr[0].position=position;
	if(UpdateColumns(tableid,tempattr)==0){

		#ifdef DEBUG
			cout<<"Successfully doing addAttribute "<<endl;
		#endif
		return 0;
	}
	#ifdef DEBUG
		cout<<"There is bug on  addAttribute "<<endl;
	#endif
	return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tableid;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	attrname.push_back("NullFlag");
	char *VarChardata=(char *) malloc(PAGE_SIZE);

	string tempstr;
	int nullflag;
	int offset = 0;



	tableid=getTableId(tableName);
	if(RelationManager::scan("Columns","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator)==0){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//skip null indicator and tableid
			memcpy(VarChardata,(char *)data+1+sizeof(int),50);
			VarCharToString(data,tempstr);
			if(tempstr.compare(attributeName)==0){
			//skip null indicatior
				nullflag=1;
				offset=1+2*sizeof(int)+tempstr.size()+3*sizeof(int);
				memcpy((char *)data+offset,&nullflag,sizeof(int));
				rbfm->openFile("Columns",filehandle);
				PrepareCatalogDescriptor("Columns",descriptor);
				if(rbfm->updateRecord(filehandle,descriptor,data,rid)==0){
					rm_ScanIterator.close();
					free(data);
					free(VarChardata);
					#ifdef DEBUG
						cout<<"Successfully dropAttribute "<<endl;
					#endif
					return 0;
				}
			}
		}

	}
	#ifdef DEBUG
		cout<<"There is bug on dropAttribute "<<endl;
	#endif
	return -1;
}
