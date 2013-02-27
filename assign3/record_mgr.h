#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct ScanHandle
{
  TableData *rel;
  void *mgmtData;
} ScanHandle;

// table management
extern RC createTable (char *name, Schema *schema);
extern RC openTable (TableData *rel, char *name);
extern RC closeTable (TableData *rel);
extern RC getNumTuples (TableData *rel);

// handling records in a table
extern RC insertRecord (TableData *rel, char *data, Record *record);
extern RC deleteRecord (RID *id);
extern RC updateRecord (TableData *rel, Record *record);
extern RC getRecord (TableData *rel, RID *id, Record *record);

// scans
extern RC startScan (TableData *rel, ScanHandle *scan, Expr *cond);
extern RC next (ScanHandle *scan, Record *record);
extern RC closeScan (ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int *keys);

// dealing with records and attribute values
extern RC createRecord (Record *record, Schema *schema);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value *value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

#endif // RECORD_MGR_H
