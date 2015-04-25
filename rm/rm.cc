
#include "rm.h"
#define DEBUG 1

RC PrepareCatalogDescriptor(string tablename,vector<Attribute> &attributes){
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

RC CreateTablesRecord(void *data,int tableid,string tablename,int systemtable){
	int offset=0;
	int size=tablename.size();
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

RC CreateColumnsRecord(void * data,int tableid, Attribute attr, int position, int nullflag){
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
RC UpdataColumns(int tableid,vector<Attribute> attributes){
	int size=attributes.size();
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle table_filehandle;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<Attribute> columndescriptor;
	RID rid;

	PrepareCatalogDescriptor("Columns",columndescriptor);
	rbfm->openFile("Columns", table_filehandle);
	for(int i=0;i<size;i++){
		CreateColumnsRecord(data,tableid,attributes[i],i+1,0);
		rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
	}
	rbfm->closeFile(table_filehandle);
	free(data);
	return 0;
}
RC CreateVarChar(string &str){
	int size=str.size();
	return -1;
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
		rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);

		tableid=2;
		CreateTablesRecord(data,tableid,"Columns",systemtable);
		rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);

		rbfm->closeFile(table_filehandle);
		//create Columns
		if((rbfm->createFile("Columns"))==0){

			UpdataColumns(1,tablesdescriptor);

			PrepareCatalogDescriptor("Columns",columnsdescriptor);
			UpdataColumns(tableid,columnsdescriptor);



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
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}
