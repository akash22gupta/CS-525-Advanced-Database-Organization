#include <stdlib.h>

#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

#define ASSERT_EQUALS_RECORDS(_l,_r, schema, message)			\
  do {									\
    ASSERT_TRUE(memcmp(_l->data,_r->data,getRecordSize(schema)) == 0, message);	\
  } while(0)

#define ASSERT_EQUALS_RECORD_IN(_l,_r, rSize, schema, message)		\
  do {									\
    int i;								\
    boolean found = false;						\
    for(i = 0; i < rSize; i++)						\
      if (memcmp(_l->data,_r[i]->data,getRecordSize(schema)) == 0)	\
	found = true;							\
    ASSERT_TRUE(0, message);						\
  } while(0)


// test methods
static void testCreateTableAndInsert (void);
static void testUpdateTable (void);
static void testScans (void);

// struct for test records
typedef struct TestRecord {
  int a;
  char *b;
  int c;
} TestRecord;

// helper methods
Record *testRecord(Schema *schema, int a, char *b, int c);
Schema *testSchema (void);
Record *fromTestRecord (Schema *schema, TestRecord in);

// test name
char *testName;

// main method
int 
main (void) 
{
  testName = "";

  testCreateTableAndInsert();
  testUpdateTable();
  testScans();
}

// ************************************************************ 
void
testCreateTableAndInsert (void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = { 
    {1, "aaaa", 3}, 
    {2, "bbbb", 2},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2}
  };
  int numInserts = 10, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test creating a new table and inserting tuples";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);
  
  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < numInserts; i++)
    {
      r = fromTestRecord(schema, inserts[i]);
      TEST_CHECK(insertRecord(table,r)); 
      rids[i] = r->id;
    }

  TEST_CHECK(closeTable(table));
  TEST_CHECK(openTable(table, "test_table_r"));

  // randomly retrieve records from the table and compare to inserted ones

  for(i = 0; i < 1000; i++)
    {
      int pos = rand() % numInserts;
      RID rid = rids[pos];
      TEST_CHECK(getRecord(table, rid, r));
      ASSERT_EQUALS_RECORDS(fromTestRecord(schema, inserts[i]), r, schema, "compare records");
    }
  
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(rids);
  free(table);
  TEST_DONE();
}

void 
testUpdateTable (void)
{
  RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
  TestRecord inserts[] = { 
    {1, "aaaa", 3}, 
    {2, "bbbb", 2},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2},
    {10, "jjjj", 5},
  };
  TestRecord updates[] = {
    {1, "iiii", 6},
    {2, "iiii", 6},
    {3, "iiii", 6}
  };
  int deletes[] = {
    9,
    6,
    7,
    8,
    5
  };
  TestRecord finalR[] = {
    {1, "iiii", 6},
    {2, "iiii", 6},
    {3, "iiii", 6},
    {4, "dddd", 3},
    {5, "eeee", 5},
  };
  int numInserts = 10, numUpdates = 3, numDeletes = 5, numFinal = 5, i;
  Record *r;
  RID *rids;
  Schema *schema;
  testName = "test creating a new table and inserting tuples";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);
  
  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < numInserts; i++)
    {
      r = fromTestRecord(schema, inserts[i]);
      TEST_CHECK(insertRecord(table,r)); 
      rids[i] = r->id;
    }

  // delete rows from table
  for(i = 0; i < numDeletes; i++)
    {
      TEST_CHECK(deleteRecord(table,rids[deletes[i]]));
    }

  // update rows into table
  for(i = 0; i < numUpdates; i++)
    {
      r = fromTestRecord(schema, updates[i]);
      r->id = rids[i];
      TEST_CHECK(updateRecord(table,r)); 
    }

  TEST_CHECK(closeTable(table));
  TEST_CHECK(openTable(table, "test_table_r"));

  // retrieve records from the table and compare to expected final stage
  for(i = 0; i < numFinal; i++)
    {
      RID rid = rids[i];
      TEST_CHECK(getRecord(table, rid, r));
      ASSERT_EQUALS_RECORDS(fromTestRecord(schema, finalR[i]), r, schema, "compare records");
    }
  
  TEST_CHECK(closeTable(table));
  TEST_CHECK(deleteTable("test_table_r"));
  TEST_CHECK(shutdownRecordManager());

  free(table);
  TEST_DONE();

}

void testScans (void)
{
  RM_TableData *table;
  TestRecord inserts[] = { 
    {1, "aaaa", 3}, 
    {2, "bbbb", 2},
    {3, "cccc", 1},
    {4, "dddd", 3},
    {5, "eeee", 5},
    {6, "ffff", 1},
    {7, "gggg", 3},
    {8, "hhhh", 3},
    {9, "iiii", 2},
    {10, "jjjj", 5},
  };
  TestRecord scanOneResult[] = { 
    {3, "cccc", 1},
    {6, "ffff", 1},
  };
  int numInserts = 10, scanSizeOne = 2, i;
  Record *r;
  RID *rids;
  Schema *schema;
  RM_ScanHandle *sc = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
  Expr *sel, *left, *right;

  testName = "test creating a new table and inserting tuples";
  schema = testSchema();
  rids = (RID *) malloc(sizeof(RID) * numInserts);
  
  TEST_CHECK(initRecordManager(NULL));
  TEST_CHECK(createTable("test_table_r",schema));
  TEST_CHECK(openTable(table, "test_table_r"));
  
  // insert rows into table
  for(i = 0; i < numInserts; i++)
    {
      r = fromTestRecord(schema, inserts[i]);
      TEST_CHECK(insertRecord(table,r)); 
      rids[i] = r->id;
    }

  TEST_CHECK(closeTable(table));
  TEST_CHECK(openTable(table, "test_table_r"));

  // run some scans
  MAKE_CONS(left, stringToValue("i1"));
  MAKE_ATTRREF(right, 2);
  MAKE_BINOP_EXPR(sel, left, right, OP_COMP_EQUAL);

  TEST_CHECK(startScan(table, sc, sel));
  while(next(sc, r) != RC_RM_NO_MORE_TUPLES)
    {
      bool found = FALSE;
      for(i = 0; i < scanSizeOne; i++)
	{
	  found = found && (memcmp(fromTestRecord(schema, scanOneResult[i])->data,r->data,getRecordSize(schema)) == 0);
	}
      ASSERT_TRUE(found, "found record in expected scan results");
    }
  TEST_CHECK(closeScan(sc));
  
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
testSchema (void)
{
  Schema *result;
  char * names[] = { "a", "b", "c" };
  DataType dt[] = { DT_INT, DT_STRING, DT_INT };
  int sizes[] = { 0, 4, 0 };
  int keys[] = {0};

  createSchema(3, names, dt, sizes, 1, keys);

  return result;
}

Record *
fromTestRecord (Schema *schema, TestRecord in)
{
  return testRecord(schema, in.a, in.b, in.c);
}

Record *
testRecord(Schema *schema, int a, char *b, int c)
{
  Record *result = (Record *) malloc(sizeof(Record));
  Value *value;

  TEST_CHECK(createRecord(result, schema));

  MAKE_VALUE(value, DT_INT, a);
  TEST_CHECK(setAttr(result, schema, 0, value));

  MAKE_STRING_VALUE(value, b);
  TEST_CHECK(setAttr(result, schema, 0, value));

  MAKE_VALUE(value, DT_INT, c);
  TEST_CHECK(setAttr(result, schema, 0, value));

  return result;
}
