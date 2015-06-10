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

AttrType Iterator::getAttrValue(vector<Attribute> attrs, string attr, void *data, void *value, bool &nullValue)
{
    assert( data != NULL); assert(value != NULL); assert(attrs.size() > 0 );
    int nullSize = ceil( (double)attrs.size() / 8 ) ;
    unsigned char nullIndicator[nullSize];
    nullValue = false;
    memcpy( &nullIndicator, data, nullSize);
    int offset = nullSize; // offset to find value
    for( int i=0; i<attrs.size(); i++){
	//size only used in this scope
	int size;
	//printf("%s %s\n",attrs[i].name.c_str(),attr.c_str());
	// check if attrs[i] is desired attribute
	if( attrs[i].name.compare( attr ) == 0 ){
	    // if null indicator is 1, no value for desired attribute
	    if( nullIndicator[i/8] & ( 1 << (7-(i%8)) ) ){
		nullValue = true;
		return attrs[i].type;
	    }
	    // get attribute value size
	    size = getAttrSize( attrs[i], (char*)data+offset );
	    memcpy( value, (char*)data+offset, size );
	    return attrs[i].type;
	}else{
	    // skip null field for increasing offset
	    if( nullIndicator[i/8] & ( 1 << (7-(i%8)) ) ) continue;
	    // calculate size for value
	    size = getAttrSize( attrs[i], (char*)data+offset );
	    offset += size;
	}

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
void Iterator::printValue(CompOp op,string leftname,AttrType leftType,void* leftvalue, string rightname, AttrType rightType,void* rightvalue)
{
    switch( op ){
	case NO_OP:
	    printf("ComOp is NO_OP\n");
	    break;
	case EQ_OP:
	    printf("ComOp is EQ_OP\n");
	    break;
	case LT_OP:
	    printf("ComOp is LT_OP\n");
	    break;
	case GT_OP:
	    printf("ComOp is GT_OP\n");
	    break;
	case LE_OP:
	    printf("ComOp is LE_OP\n");
	    break;
	case GE_OP:
	    printf("ComOp is GE_OP\n");
	    break;
	case NE_OP:
	    printf("ComOp is NE_OP\n");
	    break;
	default:
	    printf("Undefined ComOp\n");
    }
    if(leftvalue == NULL){
	printf("leftvalue is NULL\n");
    }else{
	switch( leftType ){
	    case TypeInt:
		int a;
		memcpy( &a, leftvalue, sizeof(int));
		printf("%s(TypeInt) is %d\n",leftname.c_str(),a);
		break;
	    case TypeReal:
		float fa;
		memcpy( &fa, leftvalue, sizeof(float));
		printf("%s(TypeReal) is %f\n",leftname.c_str(),fa);
		break;
	    case TypeVarChar:
		int la;
		memcpy( &la, leftvalue, sizeof(int));
		assert( la > 0 && la < 2000 );
		string sa( (char*)leftvalue+sizeof(int), la );
		printf("%s(TypeVarChar) is %s\n",leftname.c_str(),sa.c_str());
		break;
	}
    }
    if(rightvalue == NULL){
	printf("rightvalue is NULL\n");
    }else{
	switch( rightType ){
	    case TypeInt:
		int a;
		memcpy( &a, rightvalue, sizeof(int));
		printf("%s(TypeInt) is %d\n",rightname.c_str(),a);
		break;
	    case TypeReal:
		float fa;
		memcpy( &fa, rightvalue, sizeof(float));
		printf("%s(TypeReal) is %f\n",rightname.c_str(),fa);
		break;
	    case TypeVarChar:
		int la;
		memcpy( &la, rightvalue, sizeof(int));
		assert( la > 0 && la < 2000 );
		string sa( (char*)rightvalue+sizeof(int), la );
		printf("%s(TypeVarChar) is %s\n",rightname.c_str(),sa.c_str());
		break;
	}
    }
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
    bool leftnullValue;
    bool rightnullValue;
//    for(int i=0; i<attrs.size(); i++) printf("%s\n",attrs[i].name.c_str());
    while(input->getNextTuple(data) != QE_EOF){
	attrtype = getAttrValue(attrs, condition.lhsAttr, data, vleft, leftnullValue);
	if(leftnullValue) continue;
	if(condition.bRhsIsAttr){
	    //righthand-side is attribute
	    //get value for right attribute
	    getAttrValue(attrs, condition.rhsAttr, data, vright, rightnullValue);
	    if(rightnullValue) continue;
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
    free(vleft);
    free(vright);
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
    //debug = true;
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
    // get original schema
    vector<Attribute> origin_attr;
    input->getAttributes(origin_attr);
    //fetch original null indicator
    int nullSize = ceil( (double)origin_attr.size() / 8 ) ;
    unsigned char nullIndicator[nullSize];
    memcpy( &nullIndicator, tuple, nullSize);
    if(debug) printf("nullSize is %d\n",nullSize);
    //create returned null indicator
    int returnednullSize = ceil( (double)attrNames.size() / 8 ) ;
    unsigned char returnednullIndicator[returnednullSize];
    memset(returnednullIndicator,0,returnednullSize);
    if(debug) printf("returnednullSize is %d\n",returnednullSize);

    // offset to put projected record
    int offset = returnednullSize;


    // iterator through schema, get projection
    for( int i=0; i< attrNames.size(); i++){
	int targetOffset = nullSize;
	for( int j=0; j<origin_attr.size(); j++){

	    if( origin_attr[j].name.compare( attrNames[i] ) == 0 ){
		// copy projection, increases offset
		if( nullIndicator[j/8] & ( 1 << (7-(j%8)) ) ){
		    //if null value happen, create null indicator for this bit
		    unsigned char tempnull = 1 << (7-(i%8));
		    //merge with exsiting null indicator
		    returnednullIndicator[i/8] = returnednullIndicator[i/8] + tempnull;
		    if(debug) printf("returnednullIndicator[%d/8] is %d\n",i,returnednullIndicator[i/8]);

		    continue;
		}
		int size = getAttrSize( origin_attr[j],(char*)tuple+targetOffset );
		if(debug) printf("size is %d\ntargetOffset is %d\nattrname is %s\n",size,targetOffset,origin_attr[j].name.c_str());
		memcpy( (char*)data+offset, (char*)tuple+targetOffset, size );
		if(debug) printf("offset is %d\n",offset);

		offset += size;
		break;
	    }else{
		//skip null value
		if( nullIndicator[j/8] & ( 1 << (7-(j%8)) ) ){
		    if(debug) printf("rnullIndicator[%d/8] is %d\n",j,nullIndicator[j/8]);
		    continue;
		}
		int size = getAttrSize( origin_attr[j],(char*)tuple+targetOffset );
		targetOffset += size;
		if(debug) printf("targetOffset is %d\n",targetOffset);

	    }
	}
    }
    memcpy(data,returnednullIndicator,returnednullSize);
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
    /* 
       for( int i=0; i<numRecords; i++){
       void *tuple = malloc(1000);
       buffer.push_back(tuple);
       }
     */
    printf("%d\n",numRecords);
    buffer = malloc(1000*numRecords);
    finishedFlag = SUCCESS;
    leftIn->getAttributes( lAttrs );
    rightIn->getAttributes( rAttrs );
    //debug = true;
}

BNLJoin::~BNLJoin()
{
    // release all memory
    /*
       for( int i=0; i<numRecords; i++){
       free( buffer[i] );
       }
     */
    free(buffer);
}

RC BNLJoin::getNextTuple(void *data)
{
    while( joinedQueue.empty() && finishedFlag != QE_EOF){
	updateBlock();
    }
    if( !joinedQueue.empty() ){
	int i = joinedQueue.front();

	joinedQueue.pop();  
	assert( i<numRecords ); 
	memcpy( data , (char*)buffer+i*1000 , 200 );
    }

    return finishedFlag;

}

RC BNLJoin::updateBlock()
{

    // table to record outter block's tuple has been joined 
    bool joined[numRecords];
    bool nullValue;
    memset( joined, false, numRecords*sizeof(bool) );
    memset( buffer, 0 , 10000);
    int counter = 0;
    // read tuples into block
    for( int i=0; i<numRecords; i++){

	//	printf("%p\n",buffer[i]);
	finishedFlag = leftIn->getNextTuple( (char*)buffer+i*1000 );
	//	printf("%f\n",*(float*)((char*)buffer[i]+1+sizeof(int)+sizeof(int) ));
	if( finishedFlag == QE_EOF ){ break; }
	counter = i;

    }


    void *probe = malloc(1000);


    // reset iterator 
    rightIn->setIterator();


    void *rvalue = malloc(200);
    void *lvalue = malloc(200);

    while( rightIn->getNextTuple( probe ) != QE_EOF ){
	// find comparison attribute offset 
	AttrType rtype;
	if( condition.bRhsIsAttr ){
		dprintf("condition.bRhsIsAttr\n");
	    rtype = getAttrValue( rAttrs, condition.rhsAttr, probe, rvalue, nullValue);
	    dprintf("lAttrs.size is %d\nrAttrs.size is %d\n",lAttrs.size(),rAttrs.size() );

	}else{
		dprintf("!condition.bRhsIsAttr\n");
		rtype = condition.rhsValue.type;
	    memcpy( rvalue, condition.rhsValue.data, 200 );
	}


	for( int i=0; i<=counter; i++){
	    if( joined[i] ) continue;
	    // get left value
	    AttrType ltype = getAttrValue( lAttrs, condition.lhsAttr, (char*)buffer+i*1000, lvalue, nullValue);

	    assert( ltype == rtype );
	    // compare the attribtue & value
	    if( compare( condition.op, ltype, lvalue, rvalue) ){
		joined[i] = true;
		// join right record to left record 
		join( lAttrs, (char*)buffer+i*1000, rAttrs, probe );
		//		cout<< *(float*)((char*)buffer[i]+1+16) << endl;
		//		printf("%f\n",*(float*)((char*)buffer[i]+1+20));
	    }

	}


    }


    // free pointers
    free(rvalue); free(lvalue); free(probe);

    // push joined results (pointers) into queue
    for( int i=0; i<=counter; i++){
	if( joined[i] ){
	    joinedQueue.push(i);
	    //		printf("yoyoyo %d\n",*(int*)((char*)buffer[i]+1));
	}
    }





}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    vector<Attribute> left,right;
    leftIn->getAttributes(left);
    rightIn->getAttributes(right);
    attrs.insert(attrs.begin(), left.begin(), left.end() );
    attrs.insert(attrs.end(), right.begin(),right.end() );
}



GHJoin::GHJoin( Iterator *leftIn, Iterator *rightIn, const Condition &condition,const unsigned numPartitions)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    this->condition = condition; 
    this->numPartitions = numPartitions;
    bool nullValue;
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
	AttrType type = getAttrValue( lAttrs, condition.lhsAttr, data , value, nullValue );
	int hashNum = getHash( value , type , numPartitions );
	RID rid;
	rbfm->insertRecord( leftPart[hashNum], lAttrs, data, rid);
    } 

    rightIn->getAttributes(rAttrs);
    for(int i=0; i<rAttrs.size(); i++){
	rAttrsName.push_back( rAttrs[i].name );
    }
    while( rightIn->getNextTuple( data ) != QE_EOF ){
	AttrType type = getAttrValue( rAttrs, condition.rhsAttr, data , value , nullValue);
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
    bool nullValue;
    while( rpt.getNextRecord( rid, tuple ) != RBFM_EOF ){
	AttrType type = getAttrValue( rAttrs, condition.rhsAttr, tuple, value , nullValue);
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
