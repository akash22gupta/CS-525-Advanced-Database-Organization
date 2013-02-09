#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"

// error codes


// Data Types, Records, and Schemas
typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3
} DataType;

typedef struct AttrValue {
  DataType dt;
  union Value {
    int intV;
    char *stringV;
    float floatV;
    bool boolV;
  };
} AttrValue;

typedef struct RID {
  int page;
  int slot;
} RID;

typedef struct Record
{
  RID id;
  char *data;
} Record;

typedef struct Schema
{
  int numAttr;
  char **attrNames;
  DataType *dataTypes;
  int *typeLength;
  int *keyAttrs;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct TableData
{
  char *name;
  Schema *schema;
  void *mgmtData;
}

// Bookkeeping for scans
typedef struct ScanHandle
{
  TableData *rel;
  void *mgmtData;
}

// table management
RC createTable (char *name, Schema *schema);
RC openTable (TableData *rel, char *name);
RC closeTable (TableData *rel);
RC getNumTuples (TableData *rel);

// handling records in a table
RC insertRecord (TableData *rel, char *data, Record *record);
RC deleteRecord (RID *id);
RC updateRecord (TableData *rel, Record *record);
RC getRecord (TableData *rel, RID *id, Record *record);

// scans
RC startScan (TableData *rel, ScanHandle *scan);
RC next (ScanHandle *scan);
RC closeScan (ScanHandle *scan);

// dealing with schemas
int getRecordSize (Schema *schema);
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int *keys);

// dealing with records and attribute values
RC createRecord (Record *record, Schema *schema);
RC getAttr (Record *record, Schema *schema, int attrNum, AttrVal *value);
RC setAttr (Record *record, Schema *schema, int attrNum, AttrVal *value);

char *attrValueToString (AttrVal *value);
AttrVal stringToAttrValue (char *value);

// debug methods
RC printTableInfo(TableData *rel);
RC printTableContent(TableData *rel);
RC printSchema(Schema *schema);
RC printRecord(Record *record, Schema *schema);
RC printValue(Record *record, Schema *schema, int attrNum);

#endif // RECORD_MGR_H
