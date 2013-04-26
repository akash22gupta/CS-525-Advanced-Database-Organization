#include "dberror.h"
#include "contest.h"
#include "test_helper.h"
#include "record_mgr.h"
#include "tables.h"

static long workload1 (int dbsize, int numRequests, long *t);

// helpers
static Schema *schema1 ();
static Record * fromTestRecord1 (Schema *schema, TestRecord in);
static Record *testRecord1(Schema *schema, int a, char *b, int c);

static char *randomString (int size);
static int randomInt (int min, int max);

int
main (void)
{
  long curTime = 0;

  workload1(10, &curTime);
  return 0;
}

RC
workload1 (int size, long *t)
{
  Schema *schema = schema1();
  long startTime, endTime;

  // set up

  // get start time
  startTime = time();

  // create table and insert data

  //
  endTime = time();
  (*t) += (endTime - startTime) / 1000;

  // clean up
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(table);
  free(sc);
  freeExpr(sel);
  TEST_DONE();

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
fromTestRecord1 (Schema *schema, TestRecord in)
{
  return testRecord(schema, in.a, in.b, in.c);
}

Record *
testRecord1(Schema *schema, int a, char *b, int c)
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
