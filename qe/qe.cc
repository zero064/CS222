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

    unsigned char nullIndicator[nullSize];

    memcpy( nullIndicator, ldata , lstart );
    unsigned char rIndicator[rstart];
    memcpy( rIndicator, rdata , rstart );
    // setup null indicator
    for( int i = 0; i<rAttrs.size(); i++){
    	//declare tempnull for carrying null bit
    	bool tempnull = false;
    	//calculate the correct position for null bit
    	int tempoffset  = i + lAttrs.size() - 8*floor((i+lAttrs.size())/8);
//    	printf("tempoffset is %d\n",tempoffset);
    	if(rIndicator[ i/8 ] & ( 1 << 7-(i%8) ) != 0){
    		tempnull =true;

    	}
    	unsigned char tempchar = tempnull << (7-tempoffset );
    	nullIndicator[ (i+lAttrs.size())/8 ] = nullIndicator[ (i+lAttrs.size())/8 ] + tempchar;
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
size_t Iterator::getDataSize(const vector<Attribute> &recordDescriptor, const void *data, bool printFlag){


    //count non-null attribute
    int nonNull=0;
    for(int i=0;i<recordDescriptor.size();i++){
    	if(recordDescriptor[i].length){
    		nonNull++;
    	}
    }
    // get null indicator's size
    //printf("nonNull is %d\n",nonNull);

    int nullFieldsIndicatorActualSize = ceil((double) nonNull / CHAR_BIT);
	//int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT)
    int offset = 0;
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullFieldsIndicator,0,nullFieldsIndicatorActualSize);
    memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;
    int k=0;
    for(int i=0; i<recordDescriptor.size(); i++){
	Attribute attribute = recordDescriptor[i];
	string name = attribute.name;
	AttrLength length = attribute.length;
	AttrType type = attribute.type;

	if(attribute.length != 0){

	    if(printFlag) printf("%d %s %d ",i,name.c_str(),length);

	    if( nullFieldsIndicator[k/8] & (1 << (7-(k%8)) ) ){
	    	if(printFlag) printf("null\n");
	    	k++;
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
		buffer = malloc(len+1);  // null terminator
		memcpy( buffer, (char*)data+offset, len);
		offset += len;
		((char *)buffer)[len]='\0';
		if(printFlag) printf("%s\n",buffer);
		free(buffer);
		k++;
		continue;
	    }

	    std::size_t size;
	    if( type == TypeReal ){
		size = sizeof(float);
		buffer = malloc(size);
		memcpy( buffer , (char*)data+offset, size);
		offset += size;
		if(printFlag) printf("%f \n",*(float*)buffer);

	    }else{
		size = sizeof(int);
		buffer = malloc(size);
		memcpy( buffer , (char*)data+offset, size);
		offset += size;
		if(printFlag) printf("%i \n",*(int*)buffer);
	    }

	    free(buffer);
	    k++;
	}
    }

    if(printFlag) printf("given size %d\n",offset);
    free(nullFieldsIndicator);
    return offset;
}

float Iterator::transToFloat( const Attribute attr,const void* key)
{
	float tempfloat;
	int tempint;
	//if attr.type is TypeReal directly assing *(float *)key to tempfloat
	if(attr.type == TypeReal){
		tempfloat = *(float *) key;
		return tempfloat;
	}else if(attr.type == TypeInt){
		//if attr.type is TypeInt

		//assign *(int *)key tempint
		tempint = *(int *) key;
		//type conversion, (float) tempint
		tempfloat = (float) tempint;
		return tempfloat;

	}else{
	//else assert
	assert(false && "type should be TypeReal or TypeInt");
	}
}
int Iterator::VarCharToString(void *data,string &str){
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
RC Iterator::CreateVarChar(void *data,const string &str){
	int size=str.size();
	int offset=0;
	memcpy((char *)data+offset,&size,sizeof(int));
	offset+=sizeof(int);
	memcpy((char *)data+offset,str.c_str(),size);
	offset+=size;


	return 0;
}
Filter::Filter(Iterator *input, const Condition &condition  )
{

    //debug = true;
	dprintf("Filter constructor\n");
    // Get Attributes from iterator
    input->getAttributes(attrs);
    //initialize member
    this->input = input;
    this->condition = condition;

};


RC Filter::getNextTuple(void *data)
{
    void* vleft = malloc(PAGE_SIZE);
    void* vright = malloc(PAGE_SIZE);
    AttrType attrtype;
    bool leftnullValue;
    bool rightnullValue;
//    for(int i=0; i<attrs.size(); i++) printf("%s\n",attrs[i].name.c_str());
    dprintf("In Filter, before entering while loop\n");
    while(input->getNextTuple(data) != QE_EOF){
	attrtype = getAttrValue(attrs, condition.lhsAttr, data, vleft, leftnullValue);
	if(leftnullValue) continue;
	if(condition.bRhsIsAttr){
	    dprintf("condition.bRhsIsAttr\n");
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
	    dprintf("!condition.bRhsIsAttr\n");
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
    dprintf("In Filster, rc is QE_EOF\n");
    return QE_EOF;

};

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    if(debug) printf("In getAttributes beginning\n");
	attrs.clear();
    attrs = this->attrs;

};



Project::Project(Iterator *input, const vector<string> &attrNames)
{
    //debug =true;
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
    for(int i=0; i < this->attrs.size(); i++){
    	if(debug) printf("name:%s\ntype:%d\nlength:%d\nposition:%d\n",this->attrs[i].name.c_str(),this->attrs[i].type,this->attrs[i].length,this->attrs[i].position);
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
    if(rc == QE_EOF){
    	dprintf("In project's getNextTuple\nrc is  %d,no new tuple\n",rc);
    	free(tuple);
    	return rc;
    }
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
    dprintf("rc is %d\n",rc);
    return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    if(debug) printf("IN project, getAttributes\n");
	attrs.clear();
    if(debug) printf("IN project, attrs.size is  %d\nthis-attrs is %d\n",attrs.size(),this->attrs.size());
    for(int i=0; i < this->attrs.size(); i++){
    	if(debug) printf("name:%s\ntype:%d\nlength:%d\nposition:%d\n",this->attrs[i].name.c_str(),this->attrs[i].type,this->attrs[i].length,this->attrs[i].position);
    }
	attrs = this->attrs;
    if(debug) printf("IN project,End of getAttributes\n");

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

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition   )
{
	//debug = true;
	//assign value to member
	dprintf("INLJoin constructor\n");
	//leftIn->getAttributes(leftattrs);

	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	//get descriptor for leftIn and rightIn
	dprintf("before getting leftattrs\n");
	leftIn->getAttributes(leftattrs);
	dprintf("before getting rightattrs\n");
	rightIn->getAttributes(rightattrs);
	dprintf("in End of INLJoin constructor \n");
}
RC INLJoin::getNextTuple(void *data)
{
	void * leftdata =  malloc(PAGE_SIZE);
	void * rightdata =  malloc(PAGE_SIZE);
	void * leftvalue = malloc(PAGE_SIZE);
	void * rightvalue = malloc(PAGE_SIZE);
	int leftdatasize;
	int rightdatasize;
	int totaldatasize;
	int leftvaluesize;
	bool nullValue;
	bool validattr = false;
	vector<Attribute> joindescriptor;
	getAttributes(joindescriptor);
	assert(joindescriptor.size() == (leftattrs.size() + rightattrs.size()));
	Attribute attr;
	//load attr from left attrs descriptor
	for(int i = 0;i < leftattrs.size();i++){
		if(condition.lhsAttr.compare(leftattrs[i].name) == 0 && leftattrs[i].length != 0){
			//store the attribute information
			attr = leftattrs[i];
			validattr = true;
			break;
		}
	}
	assert(condition.lhsAttr.compare(attr.name) == 0 && "condition.lhsAttr.compare(attr.name) ==0");
	if(!validattr){
		printf("%s is null attribute name\n",condition.lhsAttr.c_str());
		return -1;
	}
	dprintf("before entering outter loop\n");
	//outer loop for leftIn
	while(leftIn->getNextTuple(leftdata) != QE_EOF){

		//get leftIn tuple's size
		leftdatasize = getDataSize(leftattrs,leftdata,false);
		//debug:leftsize
		dprintf("leftdatasize is %d\n",leftdatasize);
		//get lowKey and highKey for leftIn
		getAttrValue(leftattrs, condition.lhsAttr, leftdata, leftvalue, nullValue);
		//if null value, jump to next tuple
		if(nullValue){
			dprintf("nullValue\n");
			continue;
		}
		//get value's size for leftIn's tuple
		leftvaluesize = getAttrSize(attr, leftvalue);
		//reset iterator for rightIn
		dprintf("before reset Iterator\n");
		rightIn->setIterator(leftvalue, leftvalue, true, true);
		//inner loop for rightIn
		dprintf("before entering inner loop\n");
		while(rightIn->getNextTuple(rightdata) != QE_EOF){
			dprintf("entering inner loop\n");
			//get rightIn tuple's size
			rightdatasize = getDataSize(rightattrs,rightdata,false);
			//debug:rightsize
			dprintf("rightdatasize is %d\n",rightdatasize);
			//concatenate leftIn's tuple and rightIn's tuple
			join(leftattrs, leftdata, rightattrs, rightdata);
			//get data size for joined tuple
			totaldatasize = getDataSize(joindescriptor, leftdata,false);
			//copy leftdata to data
			memcpy(data,leftdata,totaldatasize);
			//free allocated memory
			free(leftdata);
			free(rightdata);
			free(leftvalue);
			free(rightvalue);
			return 0;
		}
	}
	//free allocated memory
	free(leftdata);
	free(rightdata);
	free(leftvalue);
	free(rightvalue);
	return QE_EOF;
}
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	//concatenate leftIn attribute and rightIn attribute
    attrs.clear();
    attrs.insert(attrs.begin(), leftattrs.begin(), leftattrs.end() );
    attrs.insert(attrs.end(), rightattrs.begin(),rightattrs.end() );
}
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
	//debug = true;
	this->input = input;
	input->getAttributes(attrs);
	this->aggAttr = aggAttr;
	this->op = op;
	void *key = malloc(PAGE_SIZE);
	void *data = malloc(PAGE_SIZE);
	const float indexfloat =2;
	float tempfloat;
	Aggregation tempaggr;
	bool nullValue;
	dprintf("Aggregate constructor\n");
	// No group by block
	//check whether aggAttr is int or float
	if(aggAttr.type == TypeInt || aggAttr.type == TypeReal){
	//while loop, fetch next tuple from input
		while(input->getNextTuple(data) != QE_EOF){
			//fetch key value for aggAttr
			getAttrValue(attrs, aggAttr.name,data,key, nullValue);
			//if null key value , continue
			if(nullValue){
				dprintf("nullValue");
				continue;
			}
			//transform key value of aggAttr to float value
			tempfloat = transToFloat(aggAttr, key);
			dprintf("tempfloat is %f\n",tempfloat);

			//switch on op
			switch( op ){

				case COUNT :
				//COUNT, count++
					tempaggr = floatmap[indexfloat];
					tempaggr.count += 1;
					floatmap[indexfloat] = tempaggr;
					break;
				case SUM :
				//SUM, sum++
					tempaggr = floatmap[indexfloat];
					tempaggr.sum += tempfloat;
					floatmap[indexfloat] = tempaggr;
					break;
				case AVG :
				//AVG, sum++ and count ++
					tempaggr = floatmap[indexfloat];
					tempaggr.count += 1;
					tempaggr.sum += tempfloat;
					floatmap[indexfloat] = tempaggr;
					break;
				case MIN :
				//MIN, if new value < stored value, replace it
					tempaggr = floatmap[indexfloat];
					if(tempfloat < tempaggr.min){
						tempaggr.min = tempfloat;
						floatmap[indexfloat] = tempaggr;
					}
					break;
				case MAX :
				//MAX, if new value > stored value, replace it
					tempaggr = floatmap[indexfloat];
					if(tempfloat > tempaggr.max){
						tempaggr.max = tempfloat;
						floatmap[indexfloat] = tempaggr;
					}
					break;
				default :
					dprintf("wrong op\n");
			}
		}
		//if op is AVG, calculate result
			if(op == AVG){
				tempaggr = floatmap[indexfloat];
				tempaggr.avg = tempaggr.sum / tempaggr.count;
				floatmap[indexfloat] = tempaggr;
			}
			tempaggr = floatmap[indexfloat];
			dprintf("in map\ncount is %f\nsum is %f\navg is %f\nmax is %f\nmin is %f\n",tempaggr.count,tempaggr.sum,tempaggr.avg,tempaggr.max,tempaggr.min);
	}


	//set iterator to begin
	floatIt = floatmap.begin();
	//free allocated memory
	free(data);
	free(key);



}
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op)
{
	//debug = true;
	this->input = input;
	input->getAttributes(attrs);
	this->aggAttr = aggAttr;
	this->groupAttr = groupAttr;
	isGroupby = true;
	this->op = op;
	void *key = malloc(PAGE_SIZE);
	void *data = malloc(PAGE_SIZE);
	void *VarChardata = malloc(PAGE_SIZE);
	string tempstring;
	float tempfloat_group;
	float tempfloat;
	Aggregation tempaggr;
	bool nullValue;
	dprintf("Aggregate constructor\n");
	//Group by block

	//check whether aggAttr is int or float
	if(aggAttr.type == TypeInt || aggAttr.type == TypeReal){

		//while loop, fetch next tuple from input
		while(input->getNextTuple(data) != QE_EOF){
			//fetch key value for aggAttr
			getAttrValue(attrs, aggAttr.name,data,key, nullValue);
			//if null key value , continue
			if(nullValue){
				dprintf("nullValue");
				continue;
			}
			//transform key value of aggAttr to float value
			tempfloat = transToFloat(aggAttr, key);
			dprintf("tempfloat is %f\n",tempfloat);
			//fetch key value for groupAttr
			//!reuse key  and nullValue
			getAttrValue(attrs, groupAttr.name,data,key, nullValue);
			//if null key value , continue
			if(nullValue){
				dprintf("nullValue");
				continue;
			}
			//if groupAttr is float or int,transform key value of groupAttr to float value
			if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
				tempfloat_group = transToFloat(groupAttr, key);
				dprintf("tempfloat_group is %f\n",tempfloat_group);
			}else if(groupAttr.type == TypeVarChar){
			//if groupAttr is VarChar ,transform key value of groupAttr to string
				VarCharToString(key, tempstring);
			}else{
				assert(false);
			}
			//switch on op
			switch( op ){

				case COUNT :
				//COUNT, count++
					if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
						tempaggr = floatmap[tempfloat_group];
						tempaggr.count += 1;
						floatmap[tempfloat_group] = tempaggr;
						break;
					}else if(groupAttr.type == TypeVarChar){
						tempaggr = stringmap[tempstring];
						tempaggr.count += 1;
						stringmap[tempstring] = tempaggr;
						break;
					}
				case SUM :
				//SUM, sum++
					if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
						tempaggr = floatmap[tempfloat_group];
						tempaggr.sum += tempfloat;
						floatmap[tempfloat_group] = tempaggr;
						break;
					}else if(groupAttr.type == TypeVarChar){
						tempaggr = stringmap[tempstring];
						tempaggr.sum += tempfloat;
						stringmap[tempstring] = tempaggr;
						break;
					}
				case AVG :
				//AVG, sum++ and count ++
					if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
						tempaggr = floatmap[tempfloat_group];
						tempaggr.count += 1;
						tempaggr.sum += tempfloat;
						floatmap[tempfloat_group] = tempaggr;
						break;
					}else if(groupAttr.type == TypeVarChar){
						tempaggr = stringmap[tempstring];
						tempaggr.count += 1;
						tempaggr.sum += tempfloat;
						stringmap[tempstring] = tempaggr;
						break;
					}
				case MIN :
				//MIN, if new value < stored value, replace it
					if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
						tempaggr = floatmap[tempfloat_group];
						if(tempfloat < tempaggr.min){
							tempaggr.min = tempfloat;
							floatmap[tempfloat_group] = tempaggr;
						}
						break;
					}else if(groupAttr.type == TypeVarChar){
						tempaggr = stringmap[tempstring];
						if(tempfloat < tempaggr.min){
							tempaggr.min = tempfloat;
							stringmap[tempstring] = tempaggr;
						}
						break;
					}
				case MAX :
				//MAX, if new value > stored value, replace it
					if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
						tempaggr = floatmap[tempfloat_group];
						if(tempfloat > tempaggr.max){
							tempaggr.max = tempfloat;
							floatmap[tempfloat_group] = tempaggr;
						}
						break;
					}else if(groupAttr.type == TypeVarChar){
						tempaggr = stringmap[tempstring];
						if(tempfloat > tempaggr.max){
							tempaggr.max = tempfloat;
							stringmap[tempstring] = tempaggr;
						}
						break;
					}
			}
		}

		//if op is AVG, calculate result
		if(op == AVG){
			if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
				for(FloatMap::iterator tempit=floatmap.begin();tempit != floatmap.end();tempit++){
					tempfloat_group = tempit->first;
					tempaggr = tempit->second;
					tempaggr.avg = tempaggr.sum / tempaggr.count;
					floatmap[tempfloat_group] = tempaggr;
					dprintf("in map(%f)\ncount is %f\nsum is %f\navg is %f\n",tempfloat_group,tempaggr.count,tempaggr.sum,tempaggr.avg);
				}
			}else if(groupAttr.type == TypeVarChar){
				for(StringMap::iterator tempit=stringmap.begin();tempit != stringmap.end();tempit++){
					tempstring = tempit->first;
					tempaggr = tempit->second;
					tempaggr.avg = tempaggr.sum / tempaggr.count;
					stringmap[tempstring] = tempaggr;
					dprintf("in map(%s)\ncount is %f\nsum is %f\navg is %f\n",tempstring.c_str(),tempaggr.count,tempaggr.sum,tempaggr.avg);
				}
			}

		}else{
			if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
				for(FloatMap::iterator tempit=floatmap.begin();tempit != floatmap.end();tempit++){
					tempfloat_group = tempit->first;
					tempaggr = tempit->second;
					dprintf("in map(%f)\ncount is %f\nsum is %f\navg is %f\nmax is %f\nmin is %f\n",tempfloat_group,tempaggr.count,tempaggr.sum,tempaggr.avg,tempaggr.max,tempaggr.min);

				}
			}else if(groupAttr.type == TypeVarChar){
				for(StringMap::iterator tempit=stringmap.begin();tempit != stringmap.end();tempit++){
					tempstring = tempit->first;
					tempaggr = tempit->second;
					dprintf("in map(%s)\ncount is %f\nsum is %f\navg is %f\nmax is %f\nmin is %f\n",tempstring.c_str(),tempaggr.count,tempaggr.sum,tempaggr.avg,tempaggr.max,tempaggr.min);

				}
			}

		}

	}

	//free allocated memory block
	free(data);
	free(key);
	free(VarChardata);
	//set iterator to begin
	floatIt = floatmap.begin();
	stringIt = stringmap.begin();

}
RC Aggregate::getNextTuple(void *data)
{
	dprintf("In Aggregate::getNextTuple\n");
	//create null indicator
	char nullIndicator[1];
	memset(nullIndicator,0,1);
	dprintf("nullIndicator is %d\n",nullIndicator[0]);
	//no group by
	if(!isGroupby){
		dprintf("!isGroupby\n");

		//if ite != end
		if(floatIt != floatmap.end()){
			dprintf("floatIt != floatmap.end()\n");

			//switch on op
			switch( op ){

				case COUNT :
					//COUNT, count++
					//copy null indicator
					memcpy(data,nullIndicator,1);
					memcpy((char *)data+1,&((floatIt->second).count),4);
					break;
				case SUM :
					//SUM, sum++
					//copy null indicator
					memcpy(data,nullIndicator,1);
					memcpy((char *)data+1,&((floatIt->second).sum),4);
					break;
				case AVG :
					//AVG, sum++ and count ++
					//copy null indicator
					memcpy(data,nullIndicator,1);
					memcpy((char *)data+1,&((floatIt->second).avg),4);
					break;
				case MIN :
					//MIN
					//copy null indicator
					memcpy(data,nullIndicator,1);
					memcpy((char *)data+1,&((floatIt->second).min),4);
					break;
				case MAX :
					//MAX
					//copy null indicator
					memcpy(data,nullIndicator,1);
					memcpy((char *)data+1,&((floatIt->second).max),4);
					break;

			}
			//increase iterator
			floatIt++;
			//free allocated memory block
			return 0;

		}else if(floatIt == floatmap.end()){
		//if ite == end, return QE_EOF
			dprintf("floatIt == floatmap.end()\n");

			return QE_EOF;
		//free allocated memory block
		}
	}else{
		dprintf("isGroupby\n");

		//Group by , groupAttr is float or int
		if(groupAttr.type == TypeInt || groupAttr.type == TypeReal){
			dprintf("groupAttr.type == TypeInt || groupAttr.type == TypeReal\n");

			//if ite != end
			if(floatIt != floatmap.end()){
				dprintf("floatIt != floatmap.end()\n");

				//if groupAttr is int, type conversion to tempbuffer
				int tempint;
				void *tempbuffer = malloc(PAGE_SIZE);
				int offset =0;

				if(groupAttr.type == TypeInt){
					tempint = (int) floatIt->first;
					memcpy(tempbuffer,&tempint, 4);
				}else if(groupAttr.type == TypeReal){
					memcpy(tempbuffer,&(floatIt->first),4);
				}

			//switch on op
			switch(op){
			//COUNT, count++
			case COUNT :
				//copy null indicator
				memcpy(data,nullIndicator,1);
				offset += 1;
				//copy groupAttr's value
				memcpy((char *)data+offset,tempbuffer,4);
				offset+= 4;
				memcpy((char *)data+offset,&((floatIt->second).count),4 );
				break;
				//SUM, sum++
			case SUM :
				//copy null indicator
				memcpy(data,nullIndicator,1);
				offset += 1;
				//copy groupAttr's value
				memcpy((char *)data+offset,tempbuffer,4);
				offset+= 4;
				memcpy((char *)data+offset,&((floatIt->second).sum),4 );
				break;
			//AVG, sum++ and count ++
			case AVG:
				//copy null indicator
				memcpy(data,nullIndicator,1);
				offset += 1;
				//copy groupAttr's value
				memcpy((char *)data+offset,tempbuffer,4);
				offset+= 4;
				memcpy((char *)data+offset,&((floatIt->second).avg),4 );
				break;
			//MIN
			case MIN:
				//copy null indicator
				memcpy(data,nullIndicator,1);
				offset += 1;
				//copy groupAttr's value
				memcpy((char *)data+offset,tempbuffer,4);
				offset+= 4;
				memcpy((char *)data+offset,&((floatIt->second).min),4 );
				break;
			//MAX
			case MAX :
				//copy null indicator
				memcpy(data,nullIndicator,1);
				offset += 1;
				//copy groupAttr's value
				memcpy((char *)data+offset,tempbuffer,4);
				offset+= 4;
				memcpy((char *)data+offset,&((floatIt->second).max),4 );
				break;
			}

			//increase iterator
			floatIt++;

			//free allocated memory block
			free(tempbuffer);
			return 0;
			}else if(floatIt == floatmap.end()){
				//if ite == end, return QE_EOF
				dprintf("floatIt == floatmap.end()\n");
				return QE_EOF;
				//free allocated memory block
			}
		}else if(groupAttr.type == TypeVarChar){
			dprintf("groupAttr.type == TypeVarChar\n");

			//Group by , groupAttr is string

			//if ite != end
			if(stringIt != stringmap.end()){
				dprintf("stringIt != stingmap.end()\n");

				void* tempbuffer = malloc(PAGE_SIZE);
				int offset = 0;
				//transform groupAttr to VarChar, copy to tempbuffer
				CreateVarChar(tempbuffer, stringIt->first);

				//switch on op

				switch(op){
				//COUNT, count++
				case COUNT :
					//copy null indicator
					memcpy(data,nullIndicator,1);
					offset += 1;
					//copy groupAttr's value
					memcpy((char *)data+offset,tempbuffer,sizeof(int)+(stringIt->first).size());
					offset+= (sizeof(int)+(stringIt->first).size() );
					memcpy((char *)data+offset,&((stringIt->second).count),4 );
					break;
					//SUM, sum++
				case SUM :
					//copy null indicator
					memcpy(data,nullIndicator,1);
					offset += 1;
					//copy groupAttr's value
					memcpy((char *)data+offset,tempbuffer,sizeof(int)+(stringIt->first).size());
					offset+= (sizeof(int)+(stringIt->first).size() );
					memcpy((char *)data+offset,&((stringIt->second).sum),4 );
					break;
				//AVG, sum++ and count ++
				case AVG:
					//copy null indicator
					memcpy(data,nullIndicator,1);
					offset += 1;
					//copy groupAttr's value
					memcpy((char *)data+offset,tempbuffer,sizeof(int)+(stringIt->first).size());
					offset+= (sizeof(int)+(stringIt->first).size() );
					memcpy((char *)data+offset,&((stringIt->second).avg),4 );
					break;
				//MIN
				case MIN:
					//copy null indicator
					memcpy(data,nullIndicator,1);
					offset += 1;
					//copy groupAttr's value
					memcpy((char *)data+offset,tempbuffer,sizeof(int)+(stringIt->first).size());
					offset+= (sizeof(int)+(stringIt->first).size() );
					memcpy((char *)data+offset,&((stringIt->second).min),4 );
					dprintf("MIN is %f\n",(stringIt->second).min);
					break;
				//MAX
				case MAX :
					//copy null indicator
					memcpy(data,nullIndicator,1);
					offset += 1;
					//copy groupAttr's value
					memcpy((char *)data+offset,tempbuffer,sizeof(int)+(stringIt->first).size());
					offset+= (sizeof(int)+(stringIt->first).size() );
					memcpy((char *)data+offset,&((stringIt->second).max),4 );
					break;
				}

				//increase iterator
				stringIt++;

				//free allocated memory block
				free(tempbuffer);

			}else if(stringIt == stringmap.end()){
				//if ite == end, return QE_EOF
				dprintf("stringtIt == stingmap.end()\n");
				return QE_EOF;

			}
			//free allocated memory block
		}

	}


}
void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	Attribute resultAttr;
	resultAttr.type = TypeReal;
	resultAttr.length = 4;
	switch( op ){
	case COUNT :
		resultAttr.name = "COUNT(";
		resultAttr.name += aggAttr.name;
		resultAttr.name += ")";
		break;
	case  SUM :
		resultAttr.name = "SUM(";
		resultAttr.name += aggAttr.name;
		resultAttr.name += ")";
		break;
	case AVG :
		resultAttr.name = "AVG(";
		resultAttr.name += aggAttr.name;
		resultAttr.name += ")";
		break;
	case MIN :
		resultAttr.name = "MIN(";
		resultAttr.name += aggAttr.name;
		resultAttr.name += ")";
		break;
	case MAX:
		resultAttr.name = "MAX(";
		resultAttr.name += aggAttr.name;
		resultAttr.name += ")";
		break;
	}
	if(!isGroupby){
		resultAttr.position =1;
		attrs.push_back(resultAttr);


	}else{
		resultAttr.position=2;
		attrs.push_back(groupAttr);
		attrs.push_back(resultAttr);
	}
}


GHJoin::GHJoin( Iterator *leftIn, Iterator *rightIn, const Condition &condition,const unsigned numPartitions)
{
    this->rbfm = RecordBasedFileManager::instance();
    this->condition = condition; 
    this->numPartitions = numPartitions;
    this->rpt = NULL;
    this->secHash = 14;
    bool nullValue;
    //    this->leftIn = leftIn;
    //    this->rightIn = rightIn;

    //    vector<FileHandle> leftPart;
    for( int i=0; i<numPartitions; i++){
	FileHandle fileHandle;
	string tableName = "left_join"+to_string(i);
	rbfm->createFile(tableName);
	rbfm->openFile(tableName,fileHandle);
	leftPart.push_back(fileHandle);
    }
    //    vector<FileHandle> rightPart;
    for( int i=0; i<numPartitions; i++){
	FileHandle fileHandle;
	string tableName = "right_join"+to_string(i);
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
	//printf("hash %d\n", hashNum );
	//rbfm->printRecord( lAttrs , data );

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
    getPartition();
    free(data);
    free(value);
}

RC GHJoin::getNextTuple( void *data )
{
    RID rid;
    void *tuple = malloc( 2000 );
    void *rvalue = malloc( 300 );
    void *lvalue = malloc( 300 );
    bool nullValue;
    while( rpt->getNextRecord( rid, tuple ) != RBFM_EOF ){
	AttrType rtype = getAttrValue( rAttrs, condition.rhsAttr, tuple, rvalue, nullValue);
//	if( type == TypeReal ) printf("Correct\n");
//	rbfm->printRecord( rAttrs, tuple );

	// hash version
	int hashNum = getHash( rvalue , rtype , secHash );	
	assert( hashNum < secHash && "map key counts should < sechash ");
	for( int i=0; i<lBuffer[hashNum].size(); i++){
	    AttrType ltype = getAttrValue( lAttrs, condition.lhsAttr, lBuffer[hashNum][i], lvalue , nullValue);
	    if( compare( condition.op , ltype , lvalue, rvalue ) ){
		join( lAttrs, lBuffer[hashNum][i], rAttrs, tuple );
		memcpy( data , lBuffer[hashNum][i], 200 );
		free(tuple);
		free(lvalue);
		free(rvalue);
		return SUCCESS;
	    }
	}

	/*	vector version 
	for( int i=0; i<lBuffer.size(); i++){
	    AttrType ltype = getAttrValue( lAttrs, condition.lhsAttr, lBuffer[i], lvalue , nullValue);
	    assert( rtype == ltype );
	    if( compare( condition.op , ltype , lvalue, rvalue ) ){
		join( lAttrs, lBuffer[i], rAttrs, tuple );
		memcpy( data , lBuffer[i], 200 );
		free(tuple);
		free(lvalue);
		free(rvalue);
		return SUCCESS;
	    }
	}
	*/
    }

    if( getPartition() == QE_EOF ){
	return QE_EOF;
    }
    getNextTuple( data );
    free(tuple);
    free(lvalue);
    free(rvalue);
    return SUCCESS;

}

RC GHJoin::getPartition()
{
    if( partition >= numPartitions ){
	return QE_EOF;
    }
    // free all memeory
    // vector version
    /*
    for( int i=0; i<lBuffer.size(); i++){
	free( lBuffer[i] );
    }
    */
    // hash version
    for( int i=0; i<lBuffer.size(); i++){
	for( int j=0; j<lBuffer[i].size(); j++){
	   free( lBuffer[i][j] );
	}
	lBuffer[i].clear();
    }
    lBuffer.clear();

    

    RC rc;
    RBFM_ScanIterator lpt;
    rc = rbfm->scan(leftPart[partition], lAttrs, "", NO_OP, NULL, lAttrsName, lpt); 
    assert( rc == SUCCESS );
    // Load all tuples in partition into memory
    RID rid;

    // hash version
    for( int i=0; i< secHash ; i++){
	vector<void*> inner_partition;
	lBuffer.push_back( inner_partition );
    }

    void *tuple = malloc( 2000 );
    while( lpt.getNextRecord( rid , tuple ) != RBFM_EOF ){
	void *temp = malloc( 2000 );
	memcpy( temp , tuple , 2000 );
	/*
	// vector version
	lBuffer.push_back(temp);
	*/
	
	// hash version
	void *value = malloc(300);
	bool nullValue;	
	AttrType type = getAttrValue( lAttrs, condition.lhsAttr, tuple , value , nullValue);
	int hashNum = getHash( value , type , secHash );
	lBuffer[hashNum].push_back( temp );		
	free(value);
	
    }
    lpt.close();
    
//  debug
/*
    for(int i=0; i<lBuffer.size(); i++){
	rbfm->printRecord( lAttrs, lBuffer[i] );
    }
    printf("size of lbuffer %d\n",lBuffer.size());
*/

    if( rpt != NULL ){
	rpt->close();
	delete rpt;
//	assert(false && "wtf");
    }

    rpt = new RBFM_ScanIterator();
    rc = rbfm->scan(rightPart[partition], rAttrs, "", NO_OP, NULL, rAttrsName, *rpt); 
    assert( rc == SUCCESS );
    partition++;   
//    assert(false); 
}

GHJoin::~GHJoin()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    int numPartitions = leftPart.size();
    for( int i=0; i<numPartitions; i++){
	string lTableName = "left_join"+to_string(i);
	string rTableName = "right_join"+to_string(i);
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
