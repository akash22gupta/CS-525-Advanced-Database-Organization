#include <stdlib.h>
#include <string.h>

#include "record_mgr.h"
#include "tables.h"
#include "dberror.h"
#include "expr.h"

// internals
static RC attrOffset (Schema *schema, int attrNum, int *result);

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

  result->numAttr = numAttr;
  result->attrNames = attrNames;
  result->dataTypes = dataTypes;
  result->typeLength = typeLength;
  result->keyAttrs = keys;
  result->keySize = keySize;
  
  return result;
}

RC freeSchema (Schema *schema)
{
  int i;

  for(i = 0; i < schema->numAttr; i++)
    free(schema->attrNames[i]);
  free(schema->attrNames);
  free(schema->dataTypes);
  free(schema->typeLength);
  free(schema->keyAttrs);
  free(schema);

  return RC_OK;
}

// dealing with records and attribute values

RC
createRecord (Record **record, Schema *schema)
{
  int recSize = getRecordSize(schema);
  *record = (Record *) malloc(sizeof(Record));
  (*record)->data = malloc(recSize);
  (*record)->id.page = -1;
  (*record)->id.slot = -1;

  return RC_OK;
}

RC
freeRecord(Record *record)
{
  free(record->data);
  free(record);

  return RC_OK;
}

RC
getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
  int offset;
  char *attrData;
  Value *result;

  CHECK(attrOffset(schema, attrNum, &offset));
  attrData = record->data + offset;

  switch(schema->dataTypes[attrNum])
    {
    case DT_INT:
      {
	int val = 0;
	memcpy(&val,attrData, sizeof(int));
	MAKE_VALUE(result, DT_INT, val);
      }
      break;
    case DT_STRING:
      {
	int len = schema->typeLength[attrNum];
	result = (Value *) malloc(sizeof(Value));
	result->v.stringV = (char *) malloc(len + 1);
	strncpy(result->v.stringV, attrData, len);
	result->v.stringV[len] = '\0';
      }
      break;
    case DT_FLOAT:
      {
	float val;
	memcpy(&val,attrData, sizeof(float));
	MAKE_VALUE(result, DT_FLOAT, val);
      }
      break;
    case DT_BOOL:
      {
	bool val;
	memcpy(&val,attrData, sizeof(bool));
	MAKE_VALUE(result, DT_BOOL, val);
      }
      break;
    default:
      THROW(RC_RM_UNKOWN_DATATYPE, "UNKOWN DATATYPE");
    }

  *value = result;
  return RC_OK;
}

RC
setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
  int offset;
  char *attrData;

  CHECK(attrOffset(schema, attrNum, &offset));
  attrData = record->data + offset;

  switch(schema->dataTypes[attrNum])
    {
    case DT_INT:
      memcpy(attrData, &(value->v.intV), sizeof(int));
      break;
    case DT_STRING:
      {
	int len = schema->typeLength[attrNum];
	int cpyLen = (len > strlen(value->v.stringV)) ? strlen(value->v.stringV) : len;
	memcpy(attrData, value->v.stringV, cpyLen);
	while(cpyLen < len)
	  attrData[cpyLen++] = '\0';
      }
      break;
    case DT_FLOAT:
      memcpy(attrData, &(value->v.floatV), sizeof(float));
      break;
    case DT_BOOL:
      memcpy(attrData, &(value->v.boolV), sizeof(bool));
      break;
    default:
      THROW(RC_RM_UNKOWN_DATATYPE, "UNKOWN DATATYPE");
    }

  return RC_OK;
}

RC 
attrOffset (Schema *schema, int attrNum, int *result)
{
  int offset = 0;
  int attrPos = 0;
  
  for(attrPos = 0; attrPos < attrNum; attrPos++)
    switch (schema->dataTypes[attrPos])
      {
      case DT_STRING:
	offset += schema->typeLength[attrPos];
	break;
      case DT_INT:
	offset += sizeof(int);
	break;
      case DT_FLOAT:
	offset += sizeof(float);
	break;
      case DT_BOOL:
	offset += sizeof(bool);
	break;
      }
  
  *result = offset;
  return RC_OK;
}
