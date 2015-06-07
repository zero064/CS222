
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
    int offset = 0; // offset to find value
    for( int i=0; i<attrs.size(); i++){
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
    int offset = 0; 
    for( int i=0; i<lAttrs.size(); i++){
	offset += getAttrSize( lAttrs[i], (char*)ldata+offset );
    }
    int size = 0;
    for( int i=0; i<rAttrs.size(); i++){
	size += getAttrSize( rAttrs[i], (char*)rdata+size );
    }
    memcpy( (char*)ldata + offset , rdata , size );
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

Filter::Filter(Iterator* input, const Condition &condition) {
}

// ... the rest of your implementations go here


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
    if( joinedQueue.empty() ){
	updateBlock();
    }    
    void *tmp = joinedQueue.front();
    memcpy( data , tmp , 2000 );
    joinedQueue.pop();    
}

RC BNLJoin::updateBlock()
{
    // table to record outter block's tuple has been joined 
    bool joined[numRecords];
    memset( joined, false, numRecords );
    // read tuples into block
    for( int i=0; i<numRecords; i++){
	leftIn->getNextTuple( buffer[i] );
    }

    void *probe = malloc(2000);
    
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


	for( int i=0; i<numRecords; i++){
	    if( joined[i] ) continue;
	    // get left value 
	    AttrType ltype = getAttrValue( lAttrs, condition.lhsAttr, buffer[i], lvalue);
	    assert( ltype == rtype );
	    // compare the attribtue & value
	    if( compare( condition.op, ltype, lvalue, rvalue) ){
		joined[i] = true;
		// join right record to left record 
		join( lAttrs, buffer[i], rAttrs, probe );
	    }
	    
	}
	free(rvalue); free(lvalue);
    }

    // push joined results (pointers) into queue
    for( int i=0; i<numRecords; i++){
	if( joined[i] ){
	    joinedQueue.push( buffer[i] );
	}
    }
    // reset iterator 
    rightIn->setIterator();
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
