#include <stdlib.h>

#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include "expr.h"

// table management

RC
initRecordManager (void *mgmtData)
{
  return RC_OK;
}

RC
shutdownRecordManager (void *mgmtData)
{
  return RC_OK;
}


RC
createTable (char *name, Schema *schema)
{
  return RC_OK;
}

RC
openTable (RM_TableData *rel, char *name)
{
  return RC_OK;
}

RC
closeTable (RM_TableData *rel)
{
  return RC_OK;
}

RC 
deleteTable (char *name)
{
  return RC_OK;
}

RC
getNumTuples (RM_TableData *rel)
{
  return RC_OK;
}

// handling records in a table

RC
insertRecord (RM_TableData *rel, Record *record)
{
  return RC_OK;
}



RC
deleteRecord (RM_TableData *rel, RID id)
{
  return RC_OK;
}

RC
updateRecord (RM_TableData *rel, Record *record)
{
  return RC_OK;
}

RC
getRecord (RM_TableData *rel, RID id, Record *record)
{
  return RC_OK;
}

// scans

RC
startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
  return RC_OK;
}

RC
next (RM_ScanHandle *scan, Record *record)
{
  return RC_OK;
}

RC
closeScan (RM_ScanHandle *scan)
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

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
  Schema *result = (Schema *) malloc(sizeof(Schema));

  result->attrNames = attrNames;
  result->dataTypes = dataTypes;
  result->typeLength = typeLength;
  result->keyAttrs = keys;
  result->keySize = keySize;
  
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

