#include <time.h>
#include <stdio.h>

#include "dberror.h"
#include "contest.h"
#include "test_helper.h"
#include "record_mgr.h"
#include "tables.h"

static RC completeWorkload1 (double *t, long *ios);
static RC singleWorkload1 (int dbsize, int numRequests, int numPages, double *t, long *ios);

static RC completeWorkload2 (double *t, long *ios);
static RC singleWorkload2 (int dbsize, int numRequests, int numPages, int insertPerc, int scanFreq, double *t, long *ios);

// helpers
static RC execScan (RM_TableData *table, Expr *sel, Schema *schema);

static Schema *schema1 ();
static Record *record1(Schema *schema, int a, char *b, int c);

static char *randomString (int length);
static int randomInt (int min, int max);

char *testName = "contest";

int
main (void)
{
  double curTime = 0;
  long ios = 0;

  TEST_CHECK(completeWorkload1(&curTime, &ios));
  printf("\n-------------------------------\n-- WORKLOAD 1 TOTAL: %f sec, I/Os: %ld\n\n", curTime, ios);

  curTime = 0;
  ios = 0;
  TEST_CHECK(completeWorkload2(&curTime, &ios));
  printf("\n-------------------------------\n-- WORKLOAD 2 TOTAL: %f, I/Os: %ld\n\n", curTime, ios);

  return 0;
}

RC
completeWorkload1(double *t, long *ios)
{

  // small database + large buffer = test in memory efficiency
  TEST_CHECK(singleWorkload1 (1000, 100000, 10000, t, ios));
  TEST_CHECK(singleWorkload1 (10000, 10000, 10000, t, ios));
  TEST_CHECK(singleWorkload1 (100000, 1000, 10000, t, ios));

  // large database + large buffer = test of in memory efficiency and fast access to pages in buffer manager
  TEST_CHECK(singleWorkload1 (1000000, 100, 100000, t, ios));  

  // small database + small buffer = test eviction and efficient disk access
  TEST_CHECK(singleWorkload1 (10000, 10000, 20, t, ios));  

  // large database + small buffer = test eviction and efficient disk access
  TEST_CHECK(singleWorkload1 (1000000, 1000, 100, t, ios));  

  // very large database + small buffer = stress test scaling
  TEST_CHECK(singleWorkload1 (100000000, 10, 100, t, ios));  

  return RC_OK;
}

RC
singleWorkload1 (int size, int numRequests, int numPages, double *t, long *ios)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  Schema *schema = schema1();
  time_t startTime, endTime;
  Record *r;
  int i;
  Expr *sel, *left, *right;

  // set up
  srand(0);
  TEST_CHECK(setUpContest(numPages));

  // create table and insert data
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < size; i++)
    {
      r = record1(schema, i, randomString(4), randomInt(0, size / 100));
      TEST_CHECK(insertRecord(table,r));
      freeRecord(r);
    }

  // get start time
  startTime = time(NULL);

  // read requests
  for (i = 0; i < numRequests; i++)
    {
      // roll dice to decide whether it should be a single read or sequential
      if (rand() % 10 == 9)
	{
	  Value *c;
	  MAKE_VALUE(c, DT_INT, rand() % (size / 100));
	  MAKE_CONS(left, c);
	  MAKE_ATTRREF(right, 2);
	  MAKE_BINOP_EXPR(sel, left, right, OP_COMP_EQUAL);

	  TEST_CHECK(execScan(table, sel, schema));
	  
	  freeExpr(sel);
	  printf ("|");
	}
      else
	{
	  Value *c;
	  MAKE_VALUE(c, DT_INT, rand() % size);
	  MAKE_CONS(left, c);
	  MAKE_ATTRREF(right, 0);
	  MAKE_BINOP_EXPR(sel, left, right, OP_COMP_EQUAL);

	  TEST_CHECK(execScan(table, sel, schema));
	  
	  freeExpr(sel);
	  printf ("*");
	}
    }

  // measure time
  endTime = time(NULL);
  (*t) += difftime(endTime,startTime);
  (*ios) += getContestIOs();
  printf("\nworkload 1 - N(R)=<%i>, #scans=<%i>, M=<%i>: %f sec, %ld I/Os\n", size, numRequests, numPages, *t, *ios);
  
  // clean up
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownContest());

  free(table);

  return RC_OK;
}

RC
execScan (RM_TableData *table, Expr *sel, Schema *schema)
{
  RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
  Record *r;
  RC rc = RC_OK;
  
  r = record1(schema, -1,"kkkk", -1);
  TEST_CHECK(startScan(table, sc, sel));
  while((rc = next(sc, r)) == RC_OK)
    ;
  if (rc != RC_RM_NO_MORE_TUPLES)
    TEST_CHECK(rc);

  TEST_CHECK(closeScan(sc));
  
  free(sc);
  freeRecord(r);
  
  return RC_OK;
}


RC
completeWorkload2(double *t, long *ios)
{
  // 70 - inserts, every 100th operation is a scan
  TEST_CHECK(singleWorkload2 (10000, 100000, 100, 70, 100, t, ios));
  
  // 100 inserts, every 1000th operation is a scan
  TEST_CHECK(singleWorkload2 (100, 100000, 50, 100, 1000, t, ios));

  // large database, only 10 scans 
  TEST_CHECK(singleWorkload2 (1000000, 100000, 100, 70, 10000, t, ios));

  return RC_OK;
}

RC
singleWorkload2 (int size, int numRequests, int numPages, int percInserts, int scanFreq, double *t, long *ios)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  Schema *schema = schema1();
  time_t startTime, endTime;
  Record *r;
  int i, inserted;
  Expr *sel, *left, *right;
  RID *rids = (RID *) malloc(sizeof(RID) * (numRequests + size));
  // set up
  srand(0);
  TEST_CHECK(setUpContest(numPages));
  for(i = 0; i < (numRequests + size); i++)
    {
      rids[i].page = -1;
      rids[i].slot = -1;
    }

  // create table and insert data
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(inserted = 0; inserted < size; inserted++)
    {
      r = record1(schema, inserted, randomString(4), randomInt(0, size / 100));
      TEST_CHECK(insertRecord(table,r));
      freeRecord(r);
    }

  // get start time
  startTime = time(NULL);

  // read requests
  for (i = 0; i < numRequests; i++)
    {
      // do update or scan? every 20th operation is a scan
      if (rand() % scanFreq != 0)
	{
	  // do insert or delete? 70% are inserts
	  if (rand() % 100 <= percInserts)
	    {
	       r = record1(schema, i, randomString(4), randomInt(0, size / 100));
	       TEST_CHECK(insertRecord(table,r));
	       rids[inserted++] = r->id;
	       printf("i");
	    }
	  else
	    {
	      int delPos;
	      // find not yet deleted record
	      while(rids[delPos = (rand() % inserted)].page == -1)
		;
	      TEST_CHECK(deleteRecord(table, rids[delPos]));
	      rids[delPos].page = -1;
	      printf("d");
	    }
	}
      // decided to do a scan
      else
	{
	  // roll dice to decide whether it should be a single read or sequential
	  if (rand() % 10 == 0)
	    {
	      Value *c;
	      MAKE_VALUE(c, DT_INT, rand() % (inserted / 100));
	      MAKE_CONS(left, c);
	      MAKE_ATTRREF(right, 2);
	      MAKE_BINOP_EXPR(sel, left, right, OP_COMP_EQUAL);

	      TEST_CHECK(execScan(table, sel, schema));
	  
	      freeExpr(sel);
	      printf ("|");
	    }
	  else
	    {
	      Value *c;
	      MAKE_VALUE(c, DT_INT, rand() % inserted);
	      MAKE_CONS(left, c);
	      MAKE_ATTRREF(right, 0);
	      MAKE_BINOP_EXPR(sel, left, right, OP_COMP_EQUAL);

	      TEST_CHECK(execScan(table, sel, schema));
	  
	      freeExpr(sel);
	      printf ("*");
	    }
	}
    }

  // measure time
  endTime = time(NULL);
  (*t) += difftime(endTime,startTime);
  (*ios) += getContestIOs();
  printf("\nworkload 2 - N(R)=<%i>, #scans=<%i>, M=<%i>: %f sec, %ld I/Os\n", size, numRequests, numPages, *t, *ios);

  // clean up
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownContest());

  free(table);

  return RC_OK;
}





Schema *
schema1 (void)
{
  Schema *result;
  char *names[] = { "a", "b", "c" };
  DataType dt[] = { DT_INT, DT_STRING, DT_INT };
  int sizes[] = { 0, 4, 0 };
  int keys[] = {0};
  int i;
  char **cpNames = (char **) malloc(sizeof(char*) * 3);
  DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
  int *cpSizes = (int *) malloc(sizeof(int) * 3);
  int *cpKeys = (int *) malloc(sizeof(int));

  for(i = 0; i < 3; i++)
    {
      cpNames[i] = (char *) malloc(2);
      strcpy(cpNames[i], names[i]);
    }
  memcpy(cpDt, dt, sizeof(DataType) * 3);
  memcpy(cpSizes, sizes, sizeof(int) * 3);
  memcpy(cpKeys, keys, sizeof(int));

  result = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);

  return result;
}


Record *
record1(Schema *schema, int a, char *b, int c)
{
  Record *result;
  Value *value;

  TEST_CHECK(createRecord(&result, schema));

  MAKE_VALUE(value, DT_INT, a);
  TEST_CHECK(setAttr(result, schema, 0, value));
  freeVal(value);

  MAKE_STRING_VALUE(value, b);
  TEST_CHECK(setAttr(result, schema, 1, value));
  freeVal(value);

  MAKE_VALUE(value, DT_INT, c);
  TEST_CHECK(setAttr(result, schema, 2, value));
  freeVal(value);

  return result;
}

int
randomInt (int min, int max)
{
  int range = max - min;
  if (range == 0)
    return min;
  return (rand() % range) + min;
}

char *
randomString (int length)
{
  char *result = (char *) malloc(length);
  int i;
  
  for(i = 0; i < length; result[i++] = (char) rand() % 256)
    ;
      
  return result;
}
