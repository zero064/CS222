#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <queue>
#include <map>
#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ COUNT=0, SUM, AVG, MIN, MAX } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by
//                 the characters
const int int_pinf = numeric_limits<int>::max();
const int int_ninf = numeric_limits<int>::min();
const float float_pinf = numeric_limits<float>::max();
const float float_ninf = numeric_limits<float>::min();
struct Aggregation {
	float count = 0;
	float sum = 0;
	float avg = 0;
	float max = float_ninf;
	float min = float_pinf;
};
typedef map<string,Aggregation> StringMap;
typedef map<float,Aggregation> FloatMap;
struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
    protected:
	int getAttrSize(Attribute attr, void *data);
	AttrType getAttrValue(vector<Attribute> attrs, string attr, void *data, void *value, bool &nullValue);
	bool compare(CompOp op, AttrType type, void *v1, void *v2);
	RC join( vector<Attribute> lAttrs, void *ldata, vector<Attribute>rAttrs, void *rdata);
	void printValue(CompOp op,string leftname,AttrType leftType,void* leftvalue, string rightname, AttrType rightType,void* rightvalue);
	size_t getDataSize(const vector<Attribute> &recordDescriptor, const void *data, bool printFlag);
	float transToFloat( const Attribute attr,const void* key);
	int VarCharToString(void *data,string &str);
	RC CreateVarChar(void *data,const string &str);
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator , public DebugMsg{
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data) ;
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator *input;
        Condition condition;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

};


class Project : public Iterator , public DebugMsg {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);    //{};   // vector containing attribute names
        ~Project(); //{};

        RC getNextTuple(void *data); // {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const; //{};
    private:
	Iterator *input;
	vector<string> attrNames;
	vector<Attribute> attrs;
};

// Optional for the undergraduate solo teams. 5 extra-credit points
class BNLJoin : public Iterator , public DebugMsg{
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        ); //{};
        ~BNLJoin(); //{};

        RC getNextTuple(void *data); // {return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const; // {};
    private:
	RC updateBlock();
	Iterator *leftIn;
	TableScan *rightIn;
	Condition condition;
	unsigned numRecords;
	int finishedFlag;
	RC rightFlag;
	RC noBlock;
//	vector<void*> buffer;
	void *buffer;
	void *probe;
	int leftposition;
	int bound;
	vector<Attribute> lAttrs,rAttrs;
};


class INLJoin : public Iterator , public DebugMsg{
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator* leftIn;
        IndexScan* rightIn;
        Condition condition;
        vector<Attribute> leftattrs;
        vector<Attribute> rightattrs;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      );//{};
      ~GHJoin();//{};

      RC getNextTuple(void *data);//{return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const;//{};
    
    private:
	Condition condition;
	vector<FileHandle> leftPart,rightPart;
	vector<Attribute> lAttrs,rAttrs;
	vector<string> lAttrsName,rAttrsName;
//	vector<void*> lBuffer,rBuffer;
	vector< vector<void*> > lBuffer;
	int getHash( void *value, AttrType type , int numPartitions );
	RC getPartition();
	RecordBasedFileManager *rbfm;
	RBFM_ScanIterator *rpt; // lpt;
	int partition,numPartitions, secHash;
};

class Aggregate : public Iterator , public DebugMsg{
    // Aggregation operator
    public:
        // Mandatory for graduate teams only
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone. 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );
        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator* input;
        vector<Attribute> attrs;
        bool isGroupby = false;
        Attribute aggAttr;
        Attribute groupAttr;
        AggregateOp op;
        StringMap stringmap;
        FloatMap floatmap;
    	StringMap::iterator stringIt;
    	FloatMap::iterator floatIt;


};

#endif
