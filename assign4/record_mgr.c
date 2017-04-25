#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

void schemaReadFromFile(RM_TableData *, BM_PageHandle *);
char * readSchemaName(char *);
char * readAttributeMeataData(char *);
int readTotalKeyAttr(char *);
char * readAttributeKeyData(char *);
char * getSingleAtrData(char *, int );
char ** getAtrributesNames(char *, int );
char * extractName(char *);
int extractDataType(char *);
int * getAtributesDtType(char *, int );
int extractTypeLength(char *data);
int * getAtributeSize(char *scmData, int numAtr);
int * extractKeyDt(char *data,int keyNum);
int * extractFirstFreePageSlot(char *);
char * readFreePageSlotData(char *);
int getAtrOffsetInRec(Schema *, int );

int tombStonedRIDsList[10000];//10000 is biggest numInserts Value


typedef struct TableMgmt_info{
    int sizeOfRec;
    int totalRecordInTable;
    int blkFctr;
    RID firstFreeLoc;
    RM_TableData *rm_tbl_data;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;
}TableMgmt_info;

typedef struct RM_SCAN_MGMT{
    RID recID;
    Expr *cond;
    int count;  // no of records scaned
    RM_TableData *rm_tbl_data;
    BM_PageHandle rm_pageHandle;
    BM_BufferPool rm_bufferPool;
}RM_SCAN_MGMT;


TableMgmt_info tblmgmt_info;
RM_SCAN_MGMT rmScanMgmt;
SM_FileHandle fh;
SM_PageHandle ph;
/**
 *Initialize Storage manager
 * @param mgmtData
 * @return RC_OK
 */
RC initRecordManager (void *mgmtData){
	
    initStorageManager();
    printf("*******************************Record manager Initialized********************************");
    int j;
	for(j=0;j<10000;j++)
		tombStonedRIDsList[j]= -99;
	return RC_OK;
}
/**
 * free the memory associated with buffer manager
 * @return RC_OK
 */
RC shutdownRecordManager (){
    if(ph != ((char *)0)){
        free(ph);
    }
    printf(" \n ==============Shutdown record manager success=====================");
    return RC_OK;
}

/**
 *
 *  All information/Metadata about the table will be written to page 0;
 * @param name : name of table. Table with this file name will be created
 * @param schema : schema contains all the attribute, data type, length of each field, keys needed tpo create table
 * @return : RC_OK
 *
 */
RC createTable (char *name, Schema *schema){

    if(createPageFile(name) != RC_OK){
        return 1;
    }
    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    char tableMetadata[PAGE_SIZE];
    memset(tableMetadata,'\0',PAGE_SIZE);

    sprintf(tableMetadata,"%s|",name);

    int recordSize = getRecordSize(schema);
    sprintf(tableMetadata+ strlen(tableMetadata),"%d[",schema->numAttr);
int i;
    for(i=0; i<schema->numAttr; i++){
        sprintf(tableMetadata+ strlen(tableMetadata),"(%s:%d~%d)",schema->attrNames[i],schema->dataTypes[i],schema->typeLength[i]);
    }
    sprintf(tableMetadata+ strlen(tableMetadata),"]%d{",schema->keySize);

    for(i=0; i<schema->keySize; i++){
        sprintf(tableMetadata+ strlen(tableMetadata),"%d",schema->keyAttrs[i]);
        if(i<(schema->keySize-1))
            strcat(tableMetadata,":");
    }
    strcat(tableMetadata,"}");

    tblmgmt_info.firstFreeLoc.page =1;
    tblmgmt_info.firstFreeLoc.slot =0;
    tblmgmt_info.totalRecordInTable =0;

    sprintf(tableMetadata+ strlen(tableMetadata),"$%d:%d$",tblmgmt_info.firstFreeLoc.page,tblmgmt_info.firstFreeLoc.slot);

    sprintf(tableMetadata+ strlen(tableMetadata),"?%d?",tblmgmt_info.totalRecordInTable);

    memmove(ph,tableMetadata,PAGE_SIZE);

    if (openPageFile(name, &fh) != RC_OK) {
        return 1;
    }

    if (writeBlock(0, &fh, ph)!= RC_OK) {
        return 1;
    }


    free(ph);
    return RC_OK;
}
/***
 *
 *  Using buffer manager we pin page 0 and read data from page 0. Data parsed by schemaReadFromFile method and value initialied in rel
 * @param rel : all values in rel will be intitaialed after table reaed from page 0;
 * @param name : name of table to
 * @return
 */
RC openTable (RM_TableData *rel, char *name){
    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    BM_PageHandle *h = &tblmgmt_info.pageHandle;

    BM_BufferPool *bm = &tblmgmt_info.bufferPool;

    initBufferPool(bm, name, 3, RS_FIFO, NULL);

    if(pinPage(bm, h, 0) != RC_OK){
        RC_message = "Pin page failed ";
        return RC_PIN_PAGE_FAILED;
    }


    schemaReadFromFile(rel,h);

    if(unpinPage(bm,h) != RC_OK){
        RC_message = "Unpin page failed ";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}
/***
 * write all information back to page 0. This would override exsiting data.
 * @param rel contais all imformation related to schema
 * @return
 */
RC closeTable (RM_TableData *rel){
    char tableMetadata[PAGE_SIZE];
    BM_PageHandle *page = &tblmgmt_info.pageHandle;
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;
    char *pageData;   //user for convinient to hangle page data , do not use malloc and free. its is pointer.
    memset(tableMetadata,'\0',PAGE_SIZE);

    sprintf(tableMetadata,"%s|",rel->name);

    int recordSize = tblmgmt_info.sizeOfRec;
    sprintf(tableMetadata+ strlen(tableMetadata),"%d[",rel->schema->numAttr);
int i;
    for(i=0; i<rel->schema->numAttr; i++){
        sprintf(tableMetadata+ strlen(tableMetadata),"(%s:%d~%d)",rel->schema->attrNames[i],rel->schema->dataTypes[i],rel->schema->typeLength[i]);
    }
    sprintf(tableMetadata+ strlen(tableMetadata),"]%d{",rel->schema->keySize);

    for(i=0; i<rel->schema->keySize; i++){
        sprintf(tableMetadata+ strlen(tableMetadata),"%d",rel->schema->keyAttrs[i]);
        if(i<(rel->schema->keySize-1))
            strcat(tableMetadata,":");
    }
    strcat(tableMetadata,"}");

    sprintf(tableMetadata+ strlen(tableMetadata),"$%d:%d$",tblmgmt_info.firstFreeLoc.page,tblmgmt_info.firstFreeLoc.slot);

    sprintf(tableMetadata+ strlen(tableMetadata),"?%d?",tblmgmt_info.totalRecordInTable);



    if(pinPage(bm,page,0) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }

    memmove(page->data,tableMetadata,PAGE_SIZE);

    if( markDirty(bm,page)!=RC_OK){
        RC_message = "Page 0 Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    if(  unpinPage(bm,page)!=RC_OK){
        RC_message = "Unpin Page 0 failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }


    if(shutdownBufferPool(bm) != RC_OK){
        RC_message = "Shutdown Buffer Pool Failed";
        return RC_BUFFER_SHUTDOWN_FAILED;
    }

    return RC_OK;
}
/***
 *
 *Will delete the table and all data associated with it.This mwthod will call destroyPageFile of storage manager
 * @param name : name of table to br deleted.
 * @return RC_OK
 */
RC deleteTable (char *name){
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;
    if(name == ((char *)0)){
        RC_message = "Table name can not be null ";
        return RC_NULL_IP_PARAM;
    }

    if(destroyPageFile(name) != RC_OK){
        RC_message = "Destroyt Page File Failed";
        return RC_FILE_DESTROY_FAILED;
    }

    return RC_OK;
}
/**
 *  Returns total numbers of records in table
 * @param rel contais all imformation related to schema
 * @return total no of records in table
 */
int getNumTuples (RM_TableData *rel){

    return tblmgmt_info.totalRecordInTable;
}




/***
 *  This will insert the record passed in input parameter. Record will be inserted at avialable page and slot.
 *  Next available page and slot will be updated after record inserted into file.
 *  Next availble page amd slot has mentioned in  firstFreeLoc of RID declared in TableMgmt_info
 * @param rel : contais all imformation related to schema
 * @param record : record to be inserted into file.
 * @return RC code
 */
RC insertRecord (RM_TableData *rel, Record *record){

    char *pageData;   //user for convinient to hangle page data , do not use malloc and free. its is pointer.
    int recordSize = tblmgmt_info.sizeOfRec;
    int freePageNum = tblmgmt_info.firstFreeLoc.page;  // record will be inserted at this page number
    int freeSlotNum = tblmgmt_info.firstFreeLoc.slot;  // record will be inserted at this slot
    int blockfactor = tblmgmt_info.blkFctr;
    BM_PageHandle *page = &tblmgmt_info.pageHandle;
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;

    if(freePageNum < 1 || freeSlotNum < 0){
        RC_message = "Invalid page|Slot number ";
        return RC_IVALID_PAGE_SLOT_NUM;
    }

    if(pinPage(bm,page,freePageNum) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }


    pageData = page->data;  // assigning pointer from h to pagedata for convineint

    int offset =  freeSlotNum * recordSize; //staring postion of record; slot Start from 0 position

    record->data[recordSize-1]='$';
    memcpy(pageData+offset, record->data, recordSize);

    if( markDirty(bm,page)!=RC_OK){
        RC_message = "Page Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    if(  unpinPage(bm,page)!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

    //use while retring record back

    record->id.page = freePageNum;  // storing page number for record
    record->id.slot = freeSlotNum;  // storing slot number for record


    tblmgmt_info.totalRecordInTable = tblmgmt_info.totalRecordInTable +1;

    if(freeSlotNum ==(blockfactor-1)){
        tblmgmt_info.firstFreeLoc.page=freePageNum+1;
        tblmgmt_info.firstFreeLoc.slot =0;
    }else{
        tblmgmt_info.firstFreeLoc.slot = freeSlotNum +1;
    }
    return RC_OK;
}
/**
 * This method will delete the record passed in input parameter id
 * @param rel : contais all imformation related to schema
 * @param id : ID contains page number and slot number
 * @return RC code
 */
RC deleteRecord (RM_TableData *rel, RID id){
    int recordSize = tblmgmt_info.sizeOfRec;
    int recPageNum = id.page;  // record will be searched at this page number
    int recSlotNum = id.slot;  // record will be searched at this slot
    int blockfactor = tblmgmt_info.blkFctr;
    BM_PageHandle *page = &tblmgmt_info.pageHandle;
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;


    if(pinPage(bm,page,recPageNum) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }

    int recordOffet = recSlotNum * recordSize;

    memset(page->data+recordOffet, '\0', recordSize);  // setting values to null

    tblmgmt_info.totalRecordInTable = tblmgmt_info.totalRecordInTable -1;  // reducing number of record by 1 after deleting record

    if(markDirty(bm,page)!=RC_OK){
        RC_message = "Page Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }
    if(unpinPage(bm,page)!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}
/**
 * Update record will  update the perticular record at page and slot metioned in record
 * @param rel : : contais all imformation related to schema
 * @param record : new record to be updated into file.
 * @return : RC Code
 */
RC updateRecord (RM_TableData *rel, Record *record){
    int recordSize = tblmgmt_info.sizeOfRec;
    int recPageNum = record->id.page;  // record will be searched at this page number
    int recSlotNum = record->id.slot;  // record will be searched at this slot
    int blockfactor = tblmgmt_info.blkFctr;
    BM_PageHandle *page = &tblmgmt_info.pageHandle;
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;

    if(pinPage(bm,page,recPageNum) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }
    int recordOffet = recSlotNum * recordSize;

    memcpy(page->data+recordOffet, record->data, recordSize-1);  // recordSize-1 bacause last value in record is $. which is set by us

    if( markDirty(bm,page)!=RC_OK){
        RC_message = "Page Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    if(  unpinPage(bm,page)!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}
/***
 *This method will return record at perticular page number and slot number mention in input parameter id
 *
 * @param rel : contais all imformation related to schema
 * @param id :  contains page number and slot id which is to be read
 * @param record : record data will be pointed by record
 * @return RC code
 */
RC getRecord (RM_TableData *rel, RID id, Record *record){


    int recordSize = tblmgmt_info.sizeOfRec;
    int recPageNum = id.page;  // record will be searched at this page number
    int recSlotNum = id.slot;  // record will be searched at this slot
    int blockfactor = tblmgmt_info.blkFctr;
    BM_PageHandle *page = &tblmgmt_info.pageHandle;
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;

    if(pinPage(bm,page,recPageNum) != RC_OK){
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }

    int recordOffet = recSlotNum * recordSize;  // this will give starting point of record. remember last value in record is '$' replce it wil
    memcpy(record->data, page->data+recordOffet, recordSize); // case of error check boundry condition also check for reccord->data size
    record->data[recordSize-1]='\0';

    record->id.page = recPageNum;
    record->id.slot = recSlotNum;


    if(  unpinPage(bm,page)!=RC_OK){
        RC_message = "Unpin Page failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}


// scans
/***
 *  Initialized all the parameter related to scan function.
 * @param rel : contais all imformation related to schema
 * @param scan : contains all information related to scan
 * @param cond : contain the predicate which will be used to scan the data
 * @return RC Code
 */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;

    scan->rel = rel;
    rmScanMgmt.cond=cond;
    rmScanMgmt.recID.page=1; // records starts from page 1
    rmScanMgmt.recID.slot=0; // slot starts from 0
    rmScanMgmt.count = 0;

    scan->mgmtData = &rmScanMgmt;

    return RC_OK;
}
/**
 * This will will scan every record. after reading record it checks the condition agained the scan passed as parameter
 * @param scan : : contains all information related to scan
 * @param record :  record data will be pointed by record
 * @return RC Code
 */
RC next (RM_ScanHandle *scan, Record *record){
    if(tblmgmt_info.totalRecordInTable < 1 || rmScanMgmt.count==tblmgmt_info.totalRecordInTable)
        return  RC_RM_NO_MORE_TUPLES;

    int blockfactor = tblmgmt_info.blkFctr;
    int totalTuple = tblmgmt_info.totalRecordInTable;
    BM_PageHandle *page = &tblmgmt_info.pageHandle;
    BM_BufferPool *bm = &tblmgmt_info.bufferPool;
    int curTotalRecScan = rmScanMgmt.count; // scanning start from current count to total no of records
    int curPageScan = rmScanMgmt.recID.page;  // scanning will always start from current page  till record from last page enountered
    int curSlotScan = rmScanMgmt.recID.slot;  //  scanning will always start from current slot till record enountered
    Value *queryExpResult = (Value *) malloc(sizeof(Value));
    rmScanMgmt.count = rmScanMgmt.count +1 ;

    while(curTotalRecScan<totalTuple){

        rmScanMgmt.recID.page= curPageScan;
        rmScanMgmt.recID.slot= curSlotScan;

        if(getRecord(scan->rel,rmScanMgmt.recID,record) != RC_OK){
            RC_message="Record reading failed";
        }
        curTotalRecScan = curTotalRecScan+1;   // increment counter by 1

        if (rmScanMgmt.cond != NULL){
            evalExpr(record, (scan->rel)->schema, rmScanMgmt.cond, &queryExpResult);
            if(queryExpResult->v.boolV ==1){
                record->id.page=curPageScan;
                record->id.slot=curSlotScan;
                if(curSlotScan ==(blockfactor-1)){
                    curPageScan =curPageScan +1  ;
                    curSlotScan = 0;
                }else{
                    curSlotScan = curSlotScan +1;
                }
                rmScanMgmt.recID.page= curPageScan;
                rmScanMgmt.recID.slot= curSlotScan;
                return RC_OK;
            }
        }else{
            queryExpResult->v.boolV == TRUE; // when no condition return all records
        }
        if(curSlotScan ==(blockfactor-1)){
            curPageScan =curPageScan +1  ;
            curSlotScan = 0;
        }else{
            curSlotScan = curSlotScan +1;
        }
    }
    queryExpResult->v.boolV == TRUE;
    rmScanMgmt.recID.page=1; // records starts from page 1
    rmScanMgmt.recID.slot=0; // slot starts from 0
    rmScanMgmt.count = 0;
    return  RC_RM_NO_MORE_TUPLES;

}
/***
 * this method will close the scan and reset all information in RM_SCAN_MGMT
 * @param scan : : contains all information related to scan
 * @return RC Code
 */
RC closeScan (RM_ScanHandle *scan){
    rmScanMgmt.recID.page=1; // records starts from page 1
    rmScanMgmt.recID.slot=0; // slot starts from 0
    rmScanMgmt.count = 0;
    return RC_OK;
}


// dealing with schemas

/**
 *  This will return size of record based on  data type, length of each field
 * @param schema : schema contains all the attribute, data type, length of each field, keys needed tpo create table
 * @return record size
 */
int getRecordSize (Schema *schema){
    if(schema ==((Schema *)0)){
        RC_message = "schema is not initialized.Create schema first ";
        return RC_SCHEMA_NOT_INIT;
    }
    int recordSize = 0;
int i;
    for(i=0; i<schema->numAttr; i++){
        switch(schema->dataTypes[i]){
            case DT_INT:
                recordSize = recordSize + sizeof(int);
                break;
            case DT_STRING:
                recordSize = recordSize + (sizeof(char) * schema->typeLength[i]);
                break;
            case DT_FLOAT:
                recordSize = recordSize + sizeof(float);
                break;
            case DT_BOOL:
                recordSize = recordSize + sizeof(bool);
                break;
        }
    }
    return recordSize;
}

/**
 * create the schema with given input parameter
 * @param numAttr : attributes number
 * @param attrNames : attrubutes names
 * @param dataTypes : attrubutes data type
 * @param typeLength : length of attrribute
 * @param keySize : no of keys
 * @param keys : values of keys
 * @return schema
 */

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

    Schema *schema = (Schema * ) malloc(sizeof(Schema));

    if(schema ==((Schema *)0)) {
        RC_message = "dynamic memory allocation failed | schema";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    int recordSize = getRecordSize(schema);
    tblmgmt_info.sizeOfRec = recordSize;
    tblmgmt_info.totalRecordInTable = 0;

    return schema;
}

/**
 * Free the mememory related to schema
 * @param schema :  : schema contains all the attribute, data type, length of each field, keys needed tpo create table
 * @return Rc code
 */
RC freeSchema (Schema *schema){
    free(schema);
    return RC_OK;
}


/**
 *  Create the new record with all null '\0' values
 * @param record : pointer to newly created record
 * @param schema :  : schema contains all the attribute, data type, length of each field, keys needed tpo create table
 * @return RC Code
 */
RC createRecord (Record **record, Schema *schema){

    Record *newRec = (Record *) malloc (sizeof(Record));
    if(newRec == ((Record *)0)){
        RC_message = "dynamic memory allocation failed | Record";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }

    newRec->data = (char *)malloc(sizeof(char) * tblmgmt_info.sizeOfRec);
    memset(newRec->data,'\0',sizeof(char) * tblmgmt_info.sizeOfRec);

    newRec->id.page =-1;           //set to -1 bcz it has not inserted into table/page/slot
    newRec->id.page =-1;           //set to -1 bcz it has not inserted into table/page/slot

    *record = newRec;

    return RC_OK;
}
/***
 * Free the memory related to record
 * @param record : record to be free
 * @return RC code
 */
RC freeRecord (Record *record){
    if(record == ((Record *)0)){
        RC_message = " Record is  null";
        return RC_NULL_IP_PARAM;
    }
    if(record->data != ((char *)0))
        free(record->data);
    free(record);
    return RC_OK;
}
/**
 *  return value of attribute pointed by attrnum in value
 * @param record : Contains record data
 * @param schema :  : schema contains all the attribute, data type, length of each field, keys needed tpo create table
 * @param attrNum : attribute number wich is to be retrived
 * @param value : value to be return
 * @return RC code
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int offset = getAtrOffsetInRec(schema,attrNum);

    Value *l_rec;
    int int_atr;
    float float_atr;
    int atr_size =0;
    char *subString ;


    switch (schema->dataTypes[attrNum]) {
        case DT_INT :
            atr_size = sizeof(int);
            subString= malloc(atr_size+1);     // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';          // set last byet to '\0'
            int_atr =  atoi(subString);
            MAKE_VALUE(*value, DT_INT, int_atr);
            free(subString);
            break;
        case DT_STRING :
            atr_size =sizeof(char)*schema->typeLength[attrNum];
            subString= malloc(atr_size+1);    // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';       // set last byet to '\0'

            MAKE_STRING_VALUE(*value, subString);
            free(subString);
            break;
        case DT_FLOAT :
            atr_size = sizeof(float);
            subString= malloc(atr_size+1);     // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';          // set last byet to '\0'
            float_atr =  atof(subString);

            MAKE_VALUE(*value, DT_FLOAT, float_atr);
            free(subString);
            break;
        case DT_BOOL :
            atr_size = sizeof(bool);
            subString= malloc(atr_size+1);     // one extra byte to store '\0' char
            memcpy(subString, record->data+offset, atr_size);
            subString[atr_size]='\0';          // set last byet to '\0'
            int_atr =  atoi(subString);

            MAKE_VALUE(*value, DT_BOOL, int_atr);
            free(subString);
            break;
    }

    return RC_OK;

    return RC_OK;
}
/**
 * Set value of percular attribute given in attrNum
 * @param record : pointes to the record data
 * @param schema :  : schema contains all the attribute, data type, length of each field, keys needed tpo create table
 * @param attrNum : attribute number whoes value to set or changed
 * @param value : value of attribute
 * @return Rc Code
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){

    int offset = getAtrOffsetInRec(schema,attrNum);

    char intStr[sizeof(int)+1];
    char intStrTemp[sizeof(int)+1];
    int remainder = 0,quotient = 0;
    int k = 0,j;
    bool q,r;

    memset(intStr,'0',sizeof(char)*4);
    char *hexValue ="0001";
    int number = (int)strtol(hexValue, NULL, 16);

    switch(schema->dataTypes[attrNum])
    {
        case DT_INT: {
            strRepInt(3,value->v.intV,intStr);
            sprintf(record->data + offset, "%s", intStr);
        }
            break;
        case DT_STRING: {
            int strLength =schema->typeLength[attrNum];
            sprintf(record->data + offset, "%s", value->v.stringV);
            break;
        }
        case DT_FLOAT:
            sprintf(record->data + offset,"%f" ,value->v.floatV);
            break;
        case DT_BOOL:

            strRepInt(1,value->v.boolV,intStr);
            sprintf(record->data + offset,"%s" ,intStr);
            break;
    }

    return RC_OK;
}


void strRepInt(int j,int val,  char *intStr){
    int r=0;
    int q = val;
    int last = j;
    while (q > 0 && j >= 0) {
        r = q % 10;
        q = q / 10;
        intStr[j] = intStr[j] + r;

        j--;
    }
    intStr[last+1] = '\0';

}
/***
 * update the record based on scan condtion
 * @param rel : Information about schema details
 * @param record : Record data we will  return
 * @param updaterecord : pointer to updated record
 * @param scan : : : contains all information related to scan
 * @return RC Code
 */
RC updateScan (RM_TableData *rel, Record *record, Record *updaterecord, RM_ScanHandle *scan){
    RC rc;
    while((rc = next(scan, record)) == RC_OK)
    {
        updaterecord->id.page=record->id.page;
        updaterecord->id.slot=record->id.slot;
        updateRecord(rel,updaterecord);

    }
    return RC_OK;
}
/**
 * this method parse the data  calles further method to read and parse text
 * @param rel : Information about schema details
 * @param h : pointer to page data
 */
void schemaReadFromFile(RM_TableData *rel, BM_PageHandle *h){
    char metadata[PAGE_SIZE];
    strcpy(metadata,h->data);

    char *schema_name=readSchemaName(&metadata);


    int totalAtribute = readTotalAttributes(&metadata);


    char *atrMetadata =readAttributeMeataData(&metadata);


    int totalKeyAtr = readTotalKeyAttr(&metadata);


    char *atrKeydt = readAttributeKeyData(&metadata);


    char *freeVacSlot = readFreePageSlotData(&metadata);


    char **names=getAtrributesNames(atrMetadata,totalAtribute);
    DataType *dt =   getAtributesDtType(atrMetadata,totalAtribute);
    int *sizes = getAtributeSize(atrMetadata,totalAtribute);
    int *keys =   extractKeyDt(atrKeydt,totalKeyAtr);
    int *pageSolt = extractFirstFreePageSlot(freeVacSlot);
    int totaltuples = extractTotalRecordsTab(&metadata);


    int i;
    char **cpNames = (char **) malloc(sizeof(char*) * totalAtribute);
    DataType *cpDt = (DataType *) malloc(sizeof(DataType) * totalAtribute);
    int *cpSizes = (int *) malloc(sizeof(int) * totalAtribute);
    int *cpKeys = (int *) malloc(sizeof(int)*totalKeyAtr);
    char *cpSchemaName = (char *) malloc(sizeof(char)*20);

    memset(cpSchemaName,'\0',sizeof(char)*20);
    for(i = 0; i < totalAtribute; i++)
    {
        cpNames[i] = (char *) malloc(sizeof(char) * 10);
        strcpy(cpNames[i], names[i]);
    }

    memcpy(cpDt, dt, sizeof(DataType) * totalAtribute);
    memcpy(cpSizes, sizes, sizeof(int) * totalAtribute);
    memcpy(cpKeys, keys, sizeof(int) * totalKeyAtr);
    memcpy(cpSchemaName,schema_name,strlen(schema_name));

    free(names);
    free(dt);
    free(sizes);
    free(keys);
    free(schema_name);

    Schema *schema = createSchema(totalAtribute, cpNames, cpDt, cpSizes, totalKeyAtr, cpKeys);
    rel->schema=schema;
    rel->name =cpSchemaName;

    tblmgmt_info.rm_tbl_data = rel;
    tblmgmt_info.sizeOfRec =  getRecordSize(rel->schema) + 1;   //
    tblmgmt_info.blkFctr = (PAGE_SIZE / tblmgmt_info.sizeOfRec);
    tblmgmt_info.firstFreeLoc.page =pageSolt[0];
    tblmgmt_info.firstFreeLoc.slot =pageSolt[1];
    tblmgmt_info.totalRecordInTable = totaltuples;

}


char * readSchemaName(char *scmData){
    char *tbleName = (char *) malloc(sizeof(char)*20);
    memset(tbleName,'\0',sizeof(char)*20);
    int i=0;
    for(i=0; scmData[i] != '|'; i++){
        tbleName[i]=scmData[i];
    }
    tbleName[i]='\0';
    return tbleName;
}

int readTotalAttributes(char *scmData){
    char *strNumAtr = (char *) malloc(sizeof(int)*2);
    memset(strNumAtr,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
    for(i=0; scmData[i] != '|'; i++){
    }
    i++;
    while(scmData[i] != '['){
        strNumAtr[j++]=scmData[i++];
    }
    strNumAtr[j]='\0';
    return atoi(strNumAtr);
}


int readTotalKeyAttr(char *scmData){
    char *strNumAtr = (char *) malloc(sizeof(int)*2);
    memset(strNumAtr,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
    for(i=0; scmData[i] != ']'; i++){
    }
    i++;
    while(scmData[i] != '{'){
        strNumAtr[j++]=scmData[i++];
    }
    strNumAtr[j]='\0';
    return atoi(strNumAtr);
}

char * readAttributeMeataData(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*100);
    memset(atrData,'\0',sizeof(char)*100);
    int i=0;
    while(scmData[i] != '['){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != ']'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';

    return atrData;

}


char * readAttributeKeyData(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*50);
    memset(atrData,'\0',sizeof(char)*50);
    int i=0;
    while(scmData[i] != '{'){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != '}'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';

    return atrData;
}

char * readFreePageSlotData(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*50);
    memset(atrData,'\0',sizeof(char)*50);
    int i=0;
    while(scmData[i] != '$'){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != '$'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';

    return atrData;
}


char ** getAtrributesNames(char *scmData, int numAtr){

    char ** attributesName = (char **) malloc(sizeof(char)*numAtr);
    int i;
	for(i=0; i<numAtr; i++){
        char *atrDt =getSingleAtrData(scmData,i);
        char *name = extractName(atrDt);
        attributesName[i] = malloc(sizeof(char) * strlen(name));
        strcpy(attributesName[i],name);
        free(name);
        free(atrDt);
    }
    return attributesName;
}

int * getAtributesDtType(char *scmData, int numAtr){
    int *dt_type=(int *) malloc(sizeof(int) *numAtr);
int i;
    for(i=0; i<numAtr; i++){
        char *atrDt =getSingleAtrData(scmData,i);
        dt_type[i]  = extractDataType(atrDt);

        free(atrDt);
    }
    return dt_type;
}

int * getAtributeSize(char *scmData, int numAtr){
    int *dt_size= (int *) malloc(sizeof(int) *numAtr);
int i;
    for(i=0; i<numAtr; i++){
        char *atrDt =getSingleAtrData(scmData,i);
        dt_size[i]  = extractTypeLength(atrDt);
     
        free(atrDt);
    }

    return dt_size;
}

char * getSingleAtrData(char *scmData, int atrNum){
    char *atrData = (char *) malloc(sizeof(char)*30);
    int count=0;
    int i=0;
    while(count<=atrNum){
        if(scmData[i++] == '(')
            count++;
    }
    // i++;
    int j=0;
    for(j=0;scmData[i] != ')'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';
    return atrData;
}


char * extractName(char *data){
    char *name = (char *) malloc(sizeof(char)*10);
    memset(name,'\0',sizeof(char)*10);
    int i;
    for(i=0; data[i]!=':'; i++){
        name[i] = data[i];
    }
    name[i]='\0';
    return  name;
}

int extractDataType(char *data){
    char *dtTp = (char *) malloc(sizeof(int)*2);
    memset(dtTp,'\0',sizeof(char)*10);
    int i;
    int j;
    for(i =0 ; data[i]!=':'; i++){
    }
    i++;
    for(j=0; data[i]!='~'; j++){
        dtTp[j]=data[i++];
    }
    //printf("\n data Type [%s]",dtTp);
    dtTp[j]='\0';
    int dt =atoi(dtTp);
    free(dtTp);
    return  dt;
}

int extractTypeLength(char *data){
    char *dtLen = (char *) malloc(sizeof(int)*2);
    memset(dtLen,'\0',sizeof(char)*10);
    int i;
    int j;
    for(i =0 ; data[i]!='~'; i++){
    }
    i++;
    for(j=0; data[i]!='\0'; j++){
        dtLen[j]=data[i++];
    }
    //printf("\n data Type [%s]",dtTp);
    dtLen[j]='\0';
    int dt =atoi(dtLen);
    free(dtLen);
    return  dt;
}

int * extractKeyDt(char *data,int keyNum){
    char *val = (char *) malloc(sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *keyNum);
    memset(val,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
	int k;
    for(k=0; data[k]!='\0'; k++){
        if(data[k]==':' ){
            values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
       //     printf("\n key value %d",values[j]);
            i=0;
            j++;

        }else{
            val[i++] = data[k];
        }

    }
    values[keyNum-1] =atoi(val);
  //  printf("\n key value last %d",values[keyNum-1]);

    return  values;
}


int * extractFirstFreePageSlot(char *data){
    char *val = (char *) malloc(sizeof(int)*2);
    int * values=(int *) malloc(sizeof(int) *2);
    memset(val,'\0',sizeof(int)*2);
    int i=0;
    int j=0;
	int k;
    for(k=0; data[k]!='\0'; k++){
        if(data[k]==':' ){
            values[j]=atoi(val);
            memset(val,'\0',sizeof(int)*2);
         //   printf("\n page value %d",values[j]);
            i=0;
            j++;

        }else{
            val[i++] = data[k];
        }

    }
    values[1] =atoi(val);
    printf("\n Slot %d",values[1]);
    return  values;
}

/*
 *
 *This function will extract total no of records from page ? ?
 *
 */

int extractTotalRecordsTab(char *scmData){
    char *atrData = (char *) malloc(sizeof(char)*10);
    memset(atrData,'\0',sizeof(char)*10);
    int i=0;
    while(scmData[i] != '?'){
        i++;
    }
    i++;
    int j=0;
    for(j=0;scmData[i] != '?'; j++){
        atrData[j] = scmData[i++];
    }
    atrData[j]='\0';
    // printf(" Attribute data : %s ",atrData);

    return atoi(atrData);
}

/*
 *   returns offset/string posing of perticular attribute
 *   atrnum is offset we are seeking for
 */
int getAtrOffsetInRec(Schema *schema, int atrnum){
    int offset=0;
int pos;
    for(pos=0; pos<atrnum; pos++){
        switch(schema->dataTypes[pos]){
            case DT_INT:
                offset = offset + sizeof(int);
                break;
            case DT_STRING:
                offset = offset + (sizeof(char) *  schema->typeLength[pos]);
                break;
            case DT_FLOAT:
                offset = offset + sizeof(float);
                break;
            case DT_BOOL:
                offset = offset  + sizeof(bool);
                break;
        }
    }

    return offset;
}


void printRecord(char *record, int recLen){
    int i;
	for(i=0; i<recLen; i++){
        printf("%c",record[i]);
    }
}

/*
 * Use to print print Record deatils to make sure integrity of information and persistancy data
 *
 * */
void printTableInfoDetails(TableMgmt_info *tab_info){
    printf(" \n Printing record details ");
    printf(" \n table name [%s]",tab_info->rm_tbl_data->name);
    printf(" \n Size of record [%d]",tab_info->sizeOfRec);
    printf(" \n total Records in page (blkftr) [%d]",tab_info->blkFctr);
    printf(" \n total Attributes in table [%d]",tab_info->rm_tbl_data->schema->numAttr);
    printf(" \n total Records in table [%d]",tab_info->totalRecordInTable);
    printf(" \n next available page and slot [%d:%d]",tab_info->firstFreeLoc.page,tab_info->firstFreeLoc.slot);
}

void printPageData(char *pageData){
    printf("\n Prining page Data ==>");
int i;
    for(i=0; i<PAGE_SIZE; i++){
        printf("%c",pageData[i]);
    }
    printf("\n exiting ");
    printf("\n exiting ");
}