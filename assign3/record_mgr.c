#include <stdlib.h>

#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include "expr.h"

// table management

RC
createTable (char *name, Schema *schema)
{
  return RC_OK;
}

RC
openTable (TableData *rel, char *name)
{
  return RC_OK;
}

RC
closeTable (TableData *rel)
{
  return RC_OK;
}

RC
getNumTuples (TableData *rel)
{
  return RC_OK;
}

// handling records in a table

RC
insertRecord (TableData *rel, char *data, Record *record)
{
  return RC_OK;
}

RC
deleteRecord (RID *id)
{
  return RC_OK;
}

RC
updateRecord (TableData *rel, Record *record)
{
  return RC_OK;
}

RC
getRecord (TableData *rel, RID *id, Record *record)
{
  return RC_OK;
}

// scans

RC
startScan (TableData *rel, ScanHandle *scan, Expr *cond)
{
  return RC_OK;
}

RC
next (ScanHandle *scan, Record *record)
{
  return RC_OK;
}

RC
closeScan (ScanHandle *scan)
{
  return RC_OK;
}

// dealing with schemas
 int getRecordSize (Schema *schema)
{
  int i, size = 0;
  
  for(i = 0; i < schema->numAttr; i++)
    {
      switch(schema->dataTypes[i])
	{
	case DT_INT:
	  size += sizeof(int);	  
	  break;
	case DT_FLOAT:
	  size += sizeof(float);
	  break;
	case DT_STRING:
	  size += schema->typeLength[i];
	  break;
	case DT_BOOL:
	  size += 1;
	  break;
	}
    }

  return RC_OK;
}
 Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int *keys)
{
  Schema *result = (Schema *) malloc(sizeof(Schema));

  result->attrNames = attrNames;
  result->dataTypes = dataTypes;
  result->typeLength = typeLength;
  result->keyAttrs = keys;
  
  return RC_OK;
}

// dealing with records and attribute values

RC
createRecord (Record *record, Schema *schema)
{
  

  return RC_OK;
}

RC
getAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
  return RC_OK;
}

RC
setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
  return RC_OK;
}

