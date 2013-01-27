#include "record_mgr.h"

static RC attrOffset (Schema *schema, int attrNum, int *result);

RC 
printTable(TableData *rel)
{

  return RC_OK;  
}

RC 
printSchema(Schema *schema)
{
  printf();

  return RC_OK;
}

RC 
printRecord(Record *record, Schema *schema)
{
  int i;
  
  printf("[%i-%i] (", record->id->page, record->id->slot);

  for(i = 0; i < schema->numAttr; i++)
    {
      printValue (record, schema, i);
      printf("%s", (i == 0) ? "" : ",");
    }

  printf(")");
  return RC_OK;
}

RC 
printValue(Record *record, Schema *schema, int attrNum)
{
  int offset;
  char *attrData;
  
  attrOffset(schema, attrNum, &offset);
  attrData = record->data + attrOffset;

  switch(schema->dataTypes[attrNum])
    {
    case DT_INT:
        printf("%s:%i", schema->attrNames[attrNum], *((int *) attrData));
	break;
    case DT_STRING:
      {
	char *buf;
	int len = schema->typeLength[attrNum];
	buf = (char *) malloc(len + 1);
	strcpy(buf, attrData, len);
	buf[len] = '\0';

	printf("%s:%s", schema->attrNames[attrNum], buf);
	free(buf);
      }
      break;
    case DT_FLOAT:
      printf("%s:%d", schema->attrNames[attrNum], *((float *) attrData));
      break;
    default:
      return RM_NO_PRINT_FOR_DATATYPE;
    }
  printf("%s,%s", schema->attrNames[attrNum], );
  return RC_OK;
}

RC 
attrOffset (Schema *schema, int attrNum, int *result)
{
  int offset = 0;


  *result = offset;
  return RC_OK;
}
