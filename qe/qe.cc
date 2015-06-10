#include <math.h>
#include "qe.h"

int Iterator::getAttrSize(Attribute attr, void *data)
{
    int size = -1;
    switch( attr.type ){
	case TypeInt:
	    return sizeof(int);
	case TypeReal:
	    return sizeof(float);
	case TypeVarChar:
	    memcpy( &size , data , sizeof(int) );
	//	string sa( (char*)data+4,size);
	//	printf("%s\n",sa.c_str();
	    assert( size != -1 && size < PAGE_SIZE && "wrong in reading varchar size");
	    return size+sizeof(int);
    }
}

AttrType Iterator::getAttrValue(vector<Attribute> attrs, string attr, void *data, void *value)
{
    assert( data != NULL); assert(value != NULL); assert(attrs.size() > 0 );
    int nullSize = ceil( (double)attrs.size() / 8 ) ;
    char nullIndicator[nullSize];
    memcpy( &nullIndicator, data, nullSize);
    int offset = nullSize; // offset to find value
    for( int i=0; i<attrs.size(); i++){
	// skip null field
	if( nullIndicator[i/8] & ( 1 << (7-(i%8)) ) ) continue;
	// get attribute value size 
	int size = getAttrSize( attrs[i], (char*)data+offset ); 
	// get the value and return the type of value
	if( attrs[i].name.compare( attr ) == 0 ){
	    memcpy( value, (char*)data+offset, size );
	    return attrs[i].type;
	}
	offset += size;
    }
    // shouldn't be here
    assert(false);
}

RC Iterator::join( vector<Attribute> lAttrs, void *ldata, vector<Attribute>rAttrs, void *rdata )
{
    int nullSize = ceil( (double)( lAttrs.size() + rAttrs.size() ) / 8 ) ;

    int lstart = ceil( (double)lAttrs.size() / 8.0 );
    int lsize = 0 ;
    for( int i=0; i<lAttrs.size(); i++){
	lsize += getAttrSize( lAttrs[i], (char*)ldata+lstart+lsize );
    }

    int rstart = ceil( (double)rAttrs.size() / 8.0 );
    int rsize = 0;
    for( int i=0; i<rAttrs.size(); i++){
	rsize += getAttrSize( rAttrs[i], (char*)rdata+rstart+rsize );
    }

    void *temp = malloc( 2000 );
    memcpy( temp, ldata, lstart+lsize );

    char nullIndicator[nullSize];
    memcpy( nullIndicator, ldata , lstart );
    char rIndicator[rstart];
    memcpy( rIndicator, rdata , rstart );
    // setup null indicator
    for( int i = 0; i<rAttrs.size(); i++){
	nullIndicator[ (i+lAttrs.size())/8 ] = rIndicator[ i/8 ] & ( 1 << 7-(i%8) );
    }
    memcpy( ldata, nullIndicator, nullSize );
    memcpy( (char*)ldata+nullSize, (char*)temp+lstart, lsize );  
    memcpy( (char*)ldata+nullSize+lsize , (char*)rdata+rstart , rsize );
    free(temp);
//    printf("join %f\n",*(float*)((char*)ldata+9+12));
//    printf("join %f\n",*(float*)((char*)ldata+9+8));
    return SUCCESS;
}

bool Iterator::compare(CompOp op, AttrType type, void *v1, void *v2)
{
    int compareValue;
    switch( type ){
	case TypeInt:
	    int a,b;
	    memcpy( &a, v1, sizeof(int));
	    memcpy( &b, v2, sizeof(int));
	    compareValue = a - b;
	    break;
	case TypeReal:
	    float fa,fb;
	    memcpy( &fa, v1, sizeof(float));
	    memcpy( &fb, v2, sizeof(float));
	    compareValue = ( ( fa-fb ) * 10000000.0 );
	    break;
	case TypeVarChar:
	    int la,lb;
	    memcpy( &la, v1, sizeof(int));
	    memcpy( &lb, v2, sizeof(int));
	    assert( la > 0 && la < 2000 );
	    assert( lb > 0 && lb < 2000 );
	    string sa( (char*)v1+sizeof(int), la );
	    string sb( (char*)v2+sizeof(int), lb );
	    compareValue = sa.compare(sb);
	    break;
    }

    switch( op ){
	case NO_OP:
	    return true;
	case EQ_OP:
	    if( compareValue == 0 ) return true;
	    else return false;
	case LT_OP:
	    if( compareValue < 0 ) return true;
	    else return false;
	case GT_OP:
	    if( compareValue > 0 ) return true;
	    else return false;
	case LE_OP:
	    if( compareValue <= 0 ) return true;
	    else return false;
	case GE_OP:
	    if( compareValue >= 0 ) return true;
	    else return false;
	case NE_OP:
	    if( compareValue != 0 ) return true;
	    else return false;
    }
    assert(false);

}

Filter::Filter(Iterator *input, const Condition &condition  )
{


    // Get Attributes from iterator
    input->getAttributes(attrs);
    //initialize member
    this->input = input;
    this->condition = condition;
    debug = true;

};


RC Filter::getNextTuple(void *data)
{
    void* vleft = malloc(PAGE_SIZE);
    void* vright = malloc(PAGE_SIZE);
    AttrType attrtype;

	while(input->getNextTuple(data) != QE_EOF){
		attrtype = getAttrValue(attrs, condition.lhsAttr, data, vleft);
		if(condition.bRhsIsAttr){
			//righthand-side is attribute
			//get value for right attribute
			getAttrValue(attrs, condition.rhsAttr, data, vright);
			//if tuple match predicate, break
			if(compare(condition.op, attrtype, vleft, vright)){
				free(vleft);
				free(vright);
				return 0;
			}

		}else{
			//righthand-side is value
			//if tuple match predicate, break
			if(compare(condition.op, attrtype, vleft, condition.rhsValue.data)){
				free(vleft);
				free(vright);
				return 0;
			}
		}
	}
	return QE_EOF;

};

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = this->attrs;


};



Project::Project(Iterator *input, const vector<string> &attrNames)
{
    assert( input != NULL && "Yo iterator shouldn't be null okay?\n");
    this->input = input;
    this->attrNames = attrNames;
    vector<Attribute> origin_attr;
    input->getAttributes(origin_attr);
    for( int i=0; i< attrNames.size(); i++){
	for( int j=0; j<origin_attr.size(); j++){
	    if( origin_attr[j].name.compare( attrNames[i] ) == 0 )
		attrs.push_back( origin_attr[j] );
	}
    }
}

Project::~Project()
{
   // input->close();
}

RC Project::getNextTuple(void *data)
{
    void *tuple = malloc(2000);
    RC rc;
    rc = input->getNextTuple(tuple);

    // offset to put projected record 
    int offset = 0;
    // get original schema
    vector<Attribute> origin_attr;
    input->getAttributes(origin_attr);

    // iterator through schema, get projection
    for( int i=0; i< attrNames.size(); i++){
	int targetOffset = 0;
	for( int j=0; j<origin_attr.size(); j++){
	    int size = getAttrSize( origin_attr[j],(char*)tuple+targetOffset );
	    if( origin_attr[j].name.compare( attrNames[i] ) == 0 ){
		// copy projection, increases offset 
		memcpy( (char*)data+offset, (char*)tuple+targetOffset, size );
		offset += size;
		break;
	    }
	    targetOffset += size;
	}
    }
   
    free(tuple);
    return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs = this->attrs;
}


BNLJoin::BNLJoin(Iterator *leftIn,TableScan *rightIn,const Condition &condition,const unsigned numRecords)
{
    assert( leftIn != NULL );
    assert( rightIn != NULL );
    
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    this->condition = condition;
    this->numRecords = numRecords;
    // allocate block of memory 
    for( int i=0; i<numRecords; i++){
	void *tuple = malloc(2000);
	buffer.push_back(tuple);
    }
    finishedFlag = SUCCESS;
    leftIn->getAttributes( lAttrs );
    rightIn->getAttributes( rAttrs );
}

BNLJoin::~BNLJoin()
{
    // release all memory
    for( int i=0; i<numRecords; i++){
	free( buffer[i] );
    }
}

RC BNLJoin::getNextTuple(void *data)
{
    while( joinedQueue.empty() && finishedFlag != QE_EOF){
	updateBlock();
    }
    if( !joinedQueue.empty() ){
    int i = joinedQueue.front();
    joinedQueue.pop();    
    memcpy( data , buffer[i] , 2000 );
}

}

RC BNLJoin::updateBlock()
{

    // table to record outter block's tuple has been joined 
    bool joined[numRecords];
    memset( joined, false, numRecords*sizeof(bool) );
    int counter = 0;
    // read tuples into block
    for( int i=0; i<numRecords; i++){
	finishedFlag = leftIn->getNextTuple( buffer[i] );
//	printf("%f\n",*(float*)((char*)buffer[i]+1+sizeof(int)+sizeof(int) ));
	if( finishedFlag == QE_EOF ){ assert(false); break; }
	counter = i;
    }
 
    void *probe = malloc(2000);
    // reset iterator 
    rightIn->setIterator();

    while( rightIn->getNextTuple( probe ) != QE_EOF ){
	// find comparison attribute offset 
	void *rvalue = malloc(200);
	void *lvalue = malloc(200);
	AttrType rtype;

	if( condition.bRhsIsAttr ){
	    rtype = getAttrValue( rAttrs, condition.rhsAttr, probe, rvalue);
	}else{
	    rtype = condition.rhsValue.type;
	    memcpy( rvalue, condition.rhsValue.data, 200 );
	}

//	printf("%d %d\n",*(int*)rvalue,counter);


	for( int i=0; i<=counter; i++){
	    if( joined[i] ) continue;
	    // get left value
	    AttrType ltype = getAttrValue( lAttrs, condition.lhsAttr, buffer[i], lvalue);

	    assert( ltype == rtype );
	    // compare the attribtue & value
	    if( compare( condition.op, ltype, lvalue, rvalue) ){
		joined[i] = true;
		// join right record to left record 
		join( lAttrs, buffer[i], rAttrs, probe );
		printf("%f\n",*(float*)((char*)buffer[i]+1+20));
	    }
	    
	}
	free(rvalue); free(lvalue);

    }
    printf("QB %d\n",joinedQueue.size());
    // push joined results (pointers) into queue
    for( int i=0; i<=counter; i++){
	if( joined[i] ){
	    joinedQueue.push(i);
//		printf("yoyoyo %d\n",*(int*)((char*)buffer[i]+1));
	}
    }
    printf("QA %d\n",joinedQueue.size());
    printf("finished pushed\n");

    // free probe pointer
    free(probe);


}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
    vector<Attribute> left,right;
    leftIn->getAttributes(left);
    rightIn->getAttributes(right);
    attrs.insert(attrs.begin(), left.begin(), left.end() );
    attrs.insert(attrs.end(), right.begin(),left.end() );
}



GHJoin::GHJoin( Iterator *leftIn, Iterator *rightIn, const Condition &condition,const unsigned numPartitions)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    this->condition = condition; 
    this->numPartitions = numPartitions;
//    this->leftIn = leftIn;
//    this->rightIn = rightIn;

//    vector<FileHandle> leftPart;
    for( int i=0; i<numPartitions; i++){
	FileHandle fileHandle;
	string tableName = "left_join"+('0'+i);
	rbfm->createFile(tableName);
	rbfm->openFile(tableName,fileHandle);
	leftPart.push_back(fileHandle);
    }
//    vector<FileHandle> rightPart;
    for( int i=0; i<numPartitions; i++){
	FileHandle fileHandle;
	string tableName = "right_join"+('0'+i);
	rbfm->createFile(tableName);
	rbfm->openFile(tableName,fileHandle);
	rightPart.push_back(fileHandle);
    }

    // Parti cion~~~
    void *data = malloc(2000);
    void *value = malloc(300);

    leftIn->getAttributes(lAttrs);
    for(int i=0; i<lAttrs.size(); i++){
	lAttrsName.push_back( lAttrs[i].name );
    }
    while( leftIn->getNextTuple( data ) != QE_EOF ){
	AttrType type = getAttrValue( lAttrs, condition.lhsAttr, data , value );
	int hashNum = getHash( value , type , numPartitions );
	RID rid;
	rbfm->insertRecord( leftPart[hashNum], lAttrs, data, rid);
    } 
    
    rightIn->getAttributes(rAttrs);
    for(int i=0; i<rAttrs.size(); i++){
	rAttrsName.push_back( rAttrs[i].name );
    }
    while( rightIn->getNextTuple( data ) != QE_EOF ){
	AttrType type = getAttrValue( rAttrs, condition.rhsAttr, data , value );
	int hashNum = getHash( value , type , numPartitions );
	RID rid;
	rbfm->insertRecord( rightPart[hashNum], rAttrs, data, rid);
    } 

    partition = 0;
    free(data);
    free(value);
}

RC GHJoin::getNextTuple( void *data )
{
    RID rid;
    void *tuple = malloc( 2000 );
    void *value = malloc( 300 );
    while( rpt.getNextRecord( rid, tuple ) != RBFM_EOF ){
	AttrType type = getAttrValue( rAttrs, condition.rhsAttr, tuple, value );
	for( int i=0; i<lBuffer.size(); i++){
	    if( compare( condition.op , type , lBuffer[i], value ) ){
		join( lAttrs, lBuffer[i], rAttrs, tuple );
		memcpy( data , lBuffer[i], 2000 );
		free(tuple);
		free(value);
		return SUCCESS;
	    }
	}

    }
    
    if( getPartition() == QE_EOF ){
	return QE_EOF;
    }
    getNextTuple( data );
}

RC GHJoin::getPartition()
{
    if( partition >= numPartitions ){
	return QE_EOF;
    }
    // free all memeory
    for( int i=0; i<lBuffer.size(); i++){
	free( lBuffer[i] );
    }
    lBuffer.clear();
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rc = rbfm->scan(leftPart[partition], lAttrs, "", NO_OP, NULL, lAttrsName, lpt); 
    assert( rc == SUCCESS );
    // Load all tuples in partition into memory
    RID rid;
    void *tuple = malloc( 2000 );
    while( lpt.getNextRecord( rid , tuple ) != RBFM_EOF ){
	void *temp = malloc( 2000 );
	memcpy( temp , tuple , 2000 );
	lBuffer.push_back(temp);
    }
    lpt.close();

    rc = rbfm->scan(rightPart[partition], rAttrs, "", NO_OP, NULL, rAttrsName, rpt); 
    assert( rc == SUCCESS );
    partition++;    
}

GHJoin::~GHJoin()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    int numPartitions = leftPart.size();
    for( int i=0; i<numPartitions; i++){
	rbfm->closeFile(leftPart[i]);
	rbfm->closeFile(rightPart[i]);
	string lTableName = "left_join"+('0'+i);
	string rTableName = "right_join"+('0'+i);
	rbfm->destroyFile(lTableName);
	rbfm->destroyFile(rTableName);
    }
}

int GHJoin::getHash( void *value, AttrType type , int numPartitions )
{

    switch( type ){
	case TypeInt:
	    int a;
	    memcpy( &a , value , sizeof(int));
	    return a%numPartitions;
	case TypeReal:
	    float b;
	    memcpy( &b , value , sizeof(int));
	    return (int)b%numPartitions;
	case TypeVarChar:
	    char c;
	    memcpy( &c , (char*)value+sizeof(int), sizeof(char));
	    return c%numPartitions;
    }
}


void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
//    vector<Attribute> left,right;
 //   leftIn->getAttributes(left);
  //  rightIn->getAttributes(right);
    attrs.insert(attrs.begin(), lAttrs.begin(), lAttrs.end() );
    attrs.insert(attrs.end(), rAttrs.begin(), rAttrs.end() );
}
