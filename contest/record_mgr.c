#include "record_mgr.h"
#include "storage_mgr.h"
#include "string.h"
#include "assert.h"

#include <stdlib.h>

// Static structures
typedef struct RM_DataPage
{
  int next;
  int prev;
  int prefix_bytes; // Bytes that are due to spanned row - TODO
  char data;        // To access remaining bytes in page
                    // This should be last member
} RM_DataPage;

// -1 to ignore space and end of page that is less thatn record size.
// This space shall be used when we implement record spanning
#define DATA_SIZE ((int)(PAGE_SIZE - ((&((RM_DataPage*)0)->data) - ((char*)0)) ))
#define SLOTS_PER_PAGE(sch) ( (DATA_SIZE/getActualRecordSize(sch)) )
#define FIRST_SLOT_ADDR(dp)   ((char*) &dp->data)
#define SLOT_ADDR(dp,s,sch)   (((char*) &dp->data) + (s*getActualRecordSize(sch)))

#define GET_TOMBSTONE(addr)   ((*(char*)addr)>0)
#define SET_TOMBSTONE(addr)   (*(char*)addr=1)
#define RESET_TOMBSTONE(addr) (*(char*)addr=-1)

#define MAX_FIELDNAME_LEN 64

typedef struct RM_TableMgmtData
{
    int numTuples;
    int first_free_page;

    BM_BufferPool bm;
    BM_PageHandle ph;
} RM_TableMgmtData;

typedef struct RM_ScanMgmtData
{
    BM_PageHandle ph;

    RID rid; // Current row under scan
    int scanCount; // Total tuple scanned till now
    Expr *cond;
    RM_DataPage *dp;
} RM_ScanMgmtData;

// Miscelleneous functions
static Schema* allocSchema(int numAttr, int keySize);
static void writeRecordToSlot(Schema *sch, char *slotAddr, Record *record);
static void readRecordFromSlot(Schema *sch, char *slotAddr, Record *record);
static void updateFreePageLinks(RM_TableMgmtData *tmd, RM_DataPage *dp, Schema *sch, int pageno);
static void removeFromFreePageList(RM_TableMgmtData *tmd, RM_DataPage *dp, int pageno);
static void addToFreePageList(RM_TableMgmtData *tmd, RM_DataPage *dp, int pageno);
static int searchFreeSlot(RM_DataPage *dp, Schema *sch);
static int getActualRecordSize (Schema *schema);

// Record manager
RC initRecordManager (void *mgmtData)
{
    RC rc;
    // Initialized ?
    if ((rc=isStorageManagerInitialized()) == RC_OK)
        return(rc);
 
    // Init storage manager
    initStorageManager();

    return RC_OK;
}
RC shutdownRecordManager ()
{
    RC rc;
    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);
 
    // Error if there are open tables
    if ((rc=shutdownStorageManager()) == RC_OK)
        return rc;

    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
    return getActualRecordSize(schema)-1;
}
int getActualRecordSize (Schema *schema)
{
    int recordSize= 0,i;
 
    // Calculate record size
    // Loop through schema elements and find size of block to allocate
    for (i=0; i<schema->numAttr; i++)
        recordSize+=schema->typeLength[i];

    recordSize++; // for TOMBSTONE

    return (recordSize);
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes,
                      int *typeLength, int keySize, int *keys)
{
    Schema *schema;
    int i;
 
    // Allocate Schema and return
    schema= allocSchema(numAttr, keySize);

    // Read all schema data
    for(i=0; i<schema->numAttr; i++)
    {
       strncpy(schema->attrNames[i], attrNames[i], MAX_FIELDNAME_LEN);
       schema->dataTypes[i]= dataTypes[i];
       if (i<keySize)
         schema->keyAttrs[i]= keys[i];
       switch(schema->dataTypes[i])
       {
           case DT_STRING: schema->typeLength[i]= typeLength[i]; break;
           case DT_INT: schema->typeLength[i]= sizeof(int); break;
           case DT_FLOAT: schema->typeLength[i]= sizeof(float); break;
           case DT_BOOL: schema->typeLength[i]= sizeof(bool); break;
           default:
              assert(!"No such type");
       }
    }

    return schema;
}

RC freeSchema(Schema *schema)
{
    int i;
    for(i=0; i<schema->numAttr; i++)
       free(schema->attrNames[i]);

    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema);

    return (RC_OK);
}

// Table management
RC createTable (char *name, Schema *schema)
{
    SM_FileHandle fh;
    char data[PAGE_SIZE];
    char *offset= data;
    int recLen,i;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Check limit - schema should fit in 1 page
    recLen= (4 * sizeof(int)); // numTuples, first_free_page, numAttrs, keySize
    recLen+= (schema->numAttr * (64+4+4+4)); // Name+type+len+keyAttr. Max4 for keyAttr for now.
    if (recLen > PAGE_SIZE)
        return RC_TOO_LARGE_SCHEMA;

    // Stop if record size huge
    recLen= getActualRecordSize(schema);
    if (recLen > DATA_SIZE)
        return RC_TOO_LARGE_RECORD;

    // numTuples, numattrs, keysize, freePageNo
    memset(offset, 0, PAGE_SIZE);
    *(int*)offset = 0; // For number of tuples
    offset+= sizeof(int);

    *(int*)offset = 0; // For first_free_page
    offset+= sizeof(int);

    *(int*)offset = schema->numAttr;
    offset+= sizeof(int);

    *(int*)offset = schema->keySize;
    offset+= sizeof(int);

    for(i=0; i<schema->numAttr; i++)
    {
       // types[], keylength[], keyattrs[], names \0 delimited
       strncpy(offset, schema->attrNames[i], MAX_FIELDNAME_LEN);
       offset+=MAX_FIELDNAME_LEN;

       *(int*)offset= (int) schema->dataTypes[i];
       offset+=4;

       *(int*)offset= (int) schema->typeLength[i];
       offset+=4;

       if (i<schema->keySize)
         *(int*)offset= (int) schema->keyAttrs[i];
       offset+=4;
    }

    // No need of buffer during creation
    // Create a file with 1 page table data
    if ((rc=createPageFile(name)) != RC_OK)
        return rc;

    if ((rc=openPageFile(name, &fh)) != RC_OK)
        return rc;

    if ((rc=writeBlock(0, &fh, data)) != RC_OK)
        return rc;

    if ((rc=closePageFile(&fh)) != RC_OK)
        return rc;

    return RC_OK;
}

RC openTable (RM_TableData *rel, char *name)
{
    char *offset;
    RM_TableMgmtData *tmd;
    int numAttrs, keySize, i;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Allocate RM_TableData
    tmd= (RM_TableMgmtData*) malloc( sizeof(RM_TableMgmtData) );
    rel->mgmtData= tmd;
    rel->name= strdup(name);

    // Setup BM
    if ( (rc=initBufferPool(&tmd->bm, rel->name, 1000, 
                            RS_FIFO, NULL)) != RC_OK)
        return rc;

    // Read page and prepare schema
    if ((rc=pinPage(&tmd->bm, &tmd->ph, (PageNumber)0)) != RC_OK)
        return rc;
    offset= (char*) tmd->ph.data;

    tmd->numTuples= *(int*)offset;
    offset+= sizeof(int);
    tmd->first_free_page= *(int*)offset;
    offset+= sizeof(int);
    numAttrs= *(int*)offset;
    offset+= sizeof(int);
    keySize= *(int*)offset;
    offset+= sizeof(int);

    // Allocate Schema object - call createSchema itself
    rel->schema= allocSchema(numAttrs, keySize);

    // Read all schema data
    for(i=0; i<rel->schema->numAttr; i++)
    {
       strncpy(rel->schema->attrNames[i], offset, MAX_FIELDNAME_LEN);
       offset+=MAX_FIELDNAME_LEN;

       rel->schema->dataTypes[i]= *(int*)offset;
       offset+=4;

       rel->schema->typeLength[i]= *(int*)offset;
       offset+=4;

       if (i<keySize)
         rel->schema->keyAttrs[i]= *(int*)offset;
       offset+=4;
    }

    // Unpin after reading
    unpinPage(&tmd->bm, &tmd->ph);

    return RC_OK;
}

RC closeTable (RM_TableData *rel)
{
    RM_TableMgmtData *tmd;
    char *offset;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    tmd= rel->mgmtData;

    // Read page and prepare schema
    if ((rc=pinPage(&tmd->bm, &tmd->ph, (PageNumber)0)) != RC_OK)
        return rc;
    offset= (char*) tmd->ph.data;

    markDirty(&tmd->bm, &tmd->ph);
    *(int*)offset= tmd->numTuples;
    unpinPage(&tmd->bm, &tmd->ph);

    // CloseBM
    shutdownBufferPool(&tmd->bm);
    
    // Deallocate schema
    free(rel->name);
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->typeLength);
    free(rel->schema->keyAttrs);
    free(rel->schema);
    rel->schema= NULL;
    free(rel->mgmtData);
    rel->mgmtData= NULL;

    return RC_OK;
}
RC deleteTable (char *name)
{
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);
 
    // Destroy file
    if ((rc=destroyPageFile(name)) != RC_OK)
        return rc;

    return RC_OK;
}
int getNumTuples (RM_TableData *rel)
{
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Read tuple count from mgmt data
    return(((RM_TableMgmtData*)rel->mgmtData)->numTuples);
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
    RM_TableMgmtData *tmd= rel->mgmtData;
    BM_Pool_MgmtData *pmd= tmd->bm.mgmtData;
    RID *rid= &record->id;
    RM_DataPage *dp;
    char *slotAddr;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    if (tmd->first_free_page == 0)
    {
        // add new page
        if (appendEmptyBlock(&pmd->fh) != RC_OK)
            return RC_RM_INSERT_FAILED;
        
        // Read the block and get a slot
        rid->page= pmd->fh.totalNumPages-1;

        pinPage(&tmd->bm, &tmd->ph, (PageNumber)rid->page); // Pin fails? TODO
        dp= (RM_DataPage*) tmd->ph.data;

        // We have made sure 1 tuple fits in page during create record.
        rid->slot= 0;
    }
    else // We should have space in some page
    {
        rid->page= tmd->first_free_page;
        pinPage(&tmd->bm, &tmd->ph, (PageNumber)rid->page);
        dp= (RM_DataPage*) tmd->ph.data;
        rid->slot= searchFreeSlot(dp, rel->schema);
        if (rid->slot==-1)
        {
            unpinPage(&tmd->bm, &tmd->ph);

            // add new page
            if (appendEmptyBlock(&pmd->fh) != RC_OK)
                return RC_RM_INSERT_FAILED;
            rid->page= pmd->fh.totalNumPages-1;
            pinPage(&tmd->bm, &tmd->ph, (PageNumber)rid->page);
            dp= (RM_DataPage*) tmd->ph.data;
            rid->slot= 0;
        }
    }

    // Write record now
    markDirty(&tmd->bm, &tmd->ph);
    slotAddr= SLOT_ADDR(dp,rid->slot,rel->schema);
    writeRecordToSlot(rel->schema, slotAddr, record);
    SET_TOMBSTONE(slotAddr);
    // Mark free page links
    updateFreePageLinks(tmd,dp,rel->schema, rid->page);
    unpinPage(&tmd->bm, &tmd->ph);
    tmd->numTuples++;

    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    char *slotAddr;
    RM_TableMgmtData *tmd= rel->mgmtData;
    RM_DataPage *dp;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Mark slot as free
    if (id.page == -1 || id.slot == -1)
        return RC_RM_DELETE_FAILED;

    pinPage(&tmd->bm, &tmd->ph, (PageNumber)id.page);
    dp= (RM_DataPage*) tmd->ph.data;
    slotAddr= SLOT_ADDR(dp,id.slot,rel->schema);

    markDirty(&tmd->bm, &tmd->ph);
    RESET_TOMBSTONE(slotAddr);
    // Mark free page links
    addToFreePageList(tmd, dp, id.page);
    unpinPage(&tmd->bm, &tmd->ph);

    tmd->numTuples--;

    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record)
{
    RID *rid= &record->id;
    RM_TableMgmtData *tmd= rel->mgmtData;
    RM_DataPage *dp;
    char *slotAddr;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Mark slot as free
    if (rid->page == -1 || rid->slot == -1)
        return RC_RM_UPDATE_FAILED;

    pinPage(&tmd->bm, &tmd->ph, (PageNumber)rid->page);
    dp= (RM_DataPage*) tmd->ph.data;
    slotAddr= SLOT_ADDR(dp,rid->slot,rel->schema);

    markDirty(&tmd->bm, &tmd->ph);
    writeRecordToSlot(rel->schema, slotAddr, record);
    unpinPage(&tmd->bm, &tmd->ph);

    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RM_TableMgmtData *tmd= rel->mgmtData;
    RM_DataPage *dp;
    char *slotAddr;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Mark slot as free
    if (id.page == -1 || id.slot == -1)
        return RC_RM_UPDATE_FAILED;

    pinPage(&tmd->bm, &tmd->ph, (PageNumber)id.page);
    dp= (RM_DataPage*) tmd->ph.data;
    slotAddr= SLOT_ADDR(dp,id.slot,rel->schema);
    readRecordFromSlot(rel->schema, slotAddr, record);
    unpinPage(&tmd->bm, &tmd->ph);

    record->id= id;

    return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    RM_ScanMgmtData *smd;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // Set mgmtData
    smd = (RM_ScanMgmtData*) malloc( sizeof(RM_ScanMgmtData) );
    scan->mgmtData = smd;
    smd->rid.page= -1;
    smd->rid.slot= -1;
    smd->scanCount= 0;
    smd->cond= cond; // TODO
    scan->rel= rel;

    return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record)
{
    RM_ScanMgmtData *smd= (RM_ScanMgmtData*) scan->mgmtData;
    RM_TableMgmtData *tmd= (RM_TableMgmtData*) scan->rel->mgmtData;
    char *slotAddr;
    RC rc;
    Value *result = (Value *) malloc(sizeof(Value));	\
    result->v.boolV = TRUE;
    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);
 
    // Stop if no tuples
    if (tmd->numTuples == 0)
        return RC_RM_NO_MORE_TUPLES;

   do
    {
        if (smd->scanCount == 0)
        {
            smd->rid.page= 1;
            smd->rid.slot= 0;
            pinPage(&tmd->bm, &smd->ph, (PageNumber)smd->rid.page);
            smd->dp= (RM_DataPage*) smd->ph.data;
        }
        else if (smd->scanCount == tmd->numTuples ) // Stop scan
        {
            unpinPage(&tmd->bm, &smd->ph);
            smd->rid.page= -1;
            smd->rid.slot= -1;
            smd->scanCount = 0;
            smd->dp= NULL;
            return RC_RM_NO_MORE_TUPLES;
        }
        else
        {
            int totSlots= SLOTS_PER_PAGE(scan->rel->schema);
            smd->rid.slot++;
            if (smd->rid.slot== totSlots)
            {
                smd->rid.page++;
                smd->rid.slot= 0;
                pinPage(&tmd->bm, &smd->ph, (PageNumber)smd->rid.page);
                smd->dp= (RM_DataPage*) smd->ph.data;
            }
        }
        slotAddr= SLOT_ADDR(smd->dp,smd->rid.slot,scan->rel->schema);
        readRecordFromSlot(scan->rel->schema, slotAddr, record);
        record->id.page=smd->rid.page;
        record->id.slot=smd->rid.slot;
        smd->scanCount++;

        if (smd->cond != NULL)
            evalExpr(record, (scan->rel)->schema, smd->cond, &result);

    } while (!result->v.boolV && smd->cond != NULL);

    return RC_OK;
}

RC closeScan (RM_ScanHandle *scan)
{
    RM_ScanMgmtData *smd= (RM_ScanMgmtData*) scan->mgmtData;
    RM_TableMgmtData *tmd= (RM_TableMgmtData*) scan->rel->mgmtData;
    RC rc;

    // Initialized ?
    if ((rc=isStorageManagerInitialized()) != RC_OK)
        return(rc);

    // If incomplete scan, unpin the page
    if (smd->scanCount > 0)
        unpinPage(&tmd->bm, &smd->ph);

    // Reset mgmtData
    free(scan->mgmtData);
    scan->mgmtData= NULL;

    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema)
{
    int recordSize;
    Record *rec;

    rec= (Record*) malloc( sizeof(Record) );
    recordSize= getRecordSize(schema);
    rec->data= (char*) malloc(recordSize);
    memset(rec->data, 0, recordSize);
    rec->id.page= -1;
    rec->id.slot= -1;

    *record= rec;

    return (RC_OK);
}
RC freeRecord (Record *record)
{
    free(record->data);
    free(record);

    return RC_OK;
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    void *recordOffset= record->data;
    Value *val= (Value*) malloc(sizeof(Value));
    int i;

    for (i=0; i<schema->numAttr; i++)
    {
        if (i==attrNum)
        {
            val->dt= schema->dataTypes[attrNum];
            switch(schema->dataTypes[attrNum])
            {
                case DT_STRING:
                    val->v.stringV= (char*) malloc(schema->typeLength[i]+1);
                    memcpy(val->v.stringV, recordOffset, schema->typeLength[i]);
                    val->v.stringV[schema->typeLength[i]]='\0';
                    break;
                case DT_INT:
                case DT_FLOAT:
                case DT_BOOL:
                     memcpy(&val->v, recordOffset, schema->typeLength[i]);
                     break;
                default:
                   assert(!"No such type");
            }
            break;
        }
        recordOffset+= schema->typeLength[i];
    }

    *value= val;
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    void *recordOffset= record->data;
    int i;

    for (i=0; i<schema->numAttr; i++)
    {
        if (i==attrNum)
        {
            switch(schema->dataTypes[attrNum])
            {
                case DT_STRING:
                     memcpy(recordOffset, value->v.stringV, schema->typeLength[i]);
                     break;
                case DT_INT:
                case DT_BOOL:
                case DT_FLOAT:
                     memcpy(recordOffset, &value->v, schema->typeLength[i]);
                     break;
                default:
                   assert(!"No such type");
            }
        }
        recordOffset+= schema->typeLength[i];
    }

    return RC_OK;
}

/* Below functions handle linked list tmd->first_free_page
 * representing list of pages that have space for new
 * record
 */

// Returns -1 if there are no free slots
int searchFreeSlot(RM_DataPage *dp, Schema *sch)
{
    int i;
    char *slotAddr= FIRST_SLOT_ADDR(dp);
    int totalSlots= SLOTS_PER_PAGE(sch);
    int recordSize= getActualRecordSize(sch);

    for (i=0; i<totalSlots; i++)
    {
        if (!GET_TOMBSTONE(slotAddr))
            return i;
        slotAddr+= recordSize;
    }
    return -1;
}

void addToFreePageList(RM_TableMgmtData *tmd, RM_DataPage *dp, int pageno)
{
    // return if already marked has having free space
    if (dp->next!=0)
        return;

    // I am the first page having free space
    if (tmd->first_free_page == 0)
    {
        dp->next=dp->prev= pageno;
        tmd->first_free_page= pageno;
    }
    else // Add this page to head of list
    {
        BM_PageHandle ph;
        RM_DataPage *head_page;

        // Read head block and link this page
        pinPage(&tmd->bm, &ph, (PageNumber)tmd->first_free_page);
        head_page= (RM_DataPage*) ph.data;

        markDirty(&tmd->bm, &ph);
        head_page->prev= pageno;
        unpinPage(&tmd->bm, &ph);

        dp->next= tmd->first_free_page;
        tmd->first_free_page= pageno;
        dp->prev= 0;
    }
}

void removeFromFreePageList(RM_TableMgmtData *tmd, RM_DataPage *dp, int pageno)
{
    BM_PageHandle ph;
    RM_DataPage *tmp_dp;

    // Already removed.
    if (dp->prev==0 && dp->next==0)
        return;

    // Remove from head
    if (pageno == tmd->first_free_page)
    {
        // Single node in list
        if (dp->next == 0)
        {
            dp->prev= 0;
            tmd->first_free_page= 0;
        }
        else
        {
            // Read head block and link this page
            pinPage(&tmd->bm, &ph, (PageNumber)dp->next);
            tmp_dp= (RM_DataPage*) ph.data;
            markDirty(&tmd->bm, &ph);
            tmp_dp->prev= 0;
            unpinPage(&tmd->bm, &ph);

            tmd->first_free_page= dp->next;
            dp->next=dp->prev= 0;
        }
    }
    // Remove from tail
    else if (dp->next == 0)
    {
        // Read previous block and remove myself
        pinPage(&tmd->bm, &ph, (PageNumber)dp->prev);
        tmp_dp= (RM_DataPage*) ph.data;
        markDirty(&tmd->bm, &ph);
        tmp_dp->next= 0;
        unpinPage(&tmd->bm, &ph);

        dp->next=dp->prev= 0;
    }
    else if (dp->next != 0 && dp->prev != 0)// Read from middle
    {
        BM_PageHandle ph2;
        RM_DataPage *tmp_dp2;

        // Read next block and prev block and 
        // adjust link
        pinPage(&tmd->bm, &ph, (PageNumber)dp->prev);
        pinPage(&tmd->bm, &ph2, (PageNumber)dp->next);
        tmp_dp= (RM_DataPage*) ph.data;
        tmp_dp2= (RM_DataPage*) ph2.data;

        markDirty(&tmd->bm, &ph);
        markDirty(&tmd->bm, &ph2);

        tmp_dp->next= dp->next;
        tmp_dp2->prev= dp->prev;

        unpinPage(&tmd->bm, &ph);
        unpinPage(&tmd->bm, &ph2);

        dp->next=dp->prev= 0;
    }
    else
        assert(!"Should never hit here");
}

void updateFreePageLinks(RM_TableMgmtData *tmd, RM_DataPage *dp, Schema *sch, int pageno)
{
    // Search if there are any TOMBSTONES which are free.
    if (searchFreeSlot(dp, sch) != -1)
        addToFreePageList(tmd, dp, pageno);
    else // No free space
        removeFromFreePageList(tmd, dp, pageno);
}

// Miscelleneous
static void writeRecordToSlot(Schema *sch, char *slotAddr, Record *record)
{
    // +1, -1 below is to ignore tombstone byte
    int recordSize= getRecordSize(sch);
    memcpy(slotAddr+1, record->data, recordSize);
}

static void readRecordFromSlot(Schema *sch, char *slotAddr, Record *record)
{
    // +1, -1 below is to ignore tombstone byte
    int recordSize= getRecordSize(sch);
    memcpy(record->data, slotAddr+1, recordSize);
}

static Schema* allocSchema(int numAttr, int keySize)
{
    int i;
    Schema *schema;
    schema= (Schema*) malloc(sizeof(Schema));
    schema->numAttr= numAttr;
    schema->keySize= keySize;
    schema->attrNames= (char**) malloc( sizeof(char*)*numAttr);
    schema->dataTypes= (DataType*) malloc( sizeof(DataType)*numAttr);
    schema->typeLength= (int*) malloc( sizeof(int)*numAttr);
    schema->keyAttrs= (int*) malloc( sizeof(int)*keySize);

    for(i=0; i<numAttr; i++)
       schema->attrNames[i]= (char*) malloc(MAX_FIELDNAME_LEN);

    return (schema);
}
