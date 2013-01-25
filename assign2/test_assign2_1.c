#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
static char *testName;

// check the return code and exit if it's and error
#define CHECK(code)				\
  do {						\
    int rc_internal = (code);			\
    if (rc_internal != RC_OK)			\
      {						\
      printError(rc_internal);			\
      exit(1);					\
      }						\
  } while(0);

// check whether two strings are equal
#define ASSERT_EQUALS_STRING(expected,real,message)			\
  do {									\
    if (strcmp((expected),(real)) != 0)					\
      {									\
	printf("[%s-%s-%i-%s] FAILED: expected <%s> but was <%s>: %s\n", __FILE__, testName, __LINE__, __TIME__, expected, real, message); \
	exit(1);							\
      }									\
    printf("[%s-%s-%i-%s] OK: expected <%s> and was <%s>: %s\n", __FILE__, testName, __LINE__, __TIME__, expected, real, message); \
  } while(0)


// test and helper methods
static void testCreatingAndReadingDummyPages (void);
static void createDummyPages(BM_BufferPool *bm, int num);
static void checkDummyPages(BM_BufferPool *bm, int num);

static void testReadPage (void);

// main method
int 
main (void) 
{
  initStorageManager();
  testName = "";

  testCreatingAndReadingDummyPages();

  testReadPage();
}

// create pages n with content "Page X" and read them back to check whether the content is right
void
testCreatingAndReadingDummyPages (void)
{
  BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
  BM_PageHandle *h = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
  testName = "Creating and Reading Back Dummy Pages";

  CHECK(createPageFile("testbuffer.bin"));
  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO));

  createDummyPages(bm, 22);
  checkDummyPages(bm,20);

  CHECK(shutdownBufferPool(bm));
  CHECK(destroyPageFile("testbuffer.bin"));
  free(bm);
  free(h);
}


void 
createDummyPages(BM_BufferPool *bm, int num)
{
  int i;
  BM_PageHandle *h = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
  
  for (i = 0; i < num; i++)
    {
      CHECK(pinPage(bm, h, i));
      sprintf(h->data, "%s-%i", "Page", h->pageNum);
      CHECK(markDirty(bm, h));
      CHECK(unpinPage(bm,h));
    }
  CHECK(shutdownBufferPool(bm));
}

void 
checkDummyPages(BM_BufferPool *bm, int num)
{
  int i;
  BM_PageHandle *h = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
  char *expected = malloc(sizeof(char) * 512);

  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO));
  printPoolContent(bm);

  for (i = 0; i < num; i++)
    {
      CHECK(pinPage(bm, h, i));

      sprintf(expected, "%s-%i", "Page", h->pageNum);
      ASSERT_EQUALS_STRING(expected, h->data, "reading back dummy page content");

      CHECK(unpinPage(bm,h));
    }

  free(expected);
}

void
testReadPage ()
{
  BM_BufferPool *bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
  BM_PageHandle *h = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
  testName = "Reading a page";

  CHECK(createPageFile("testbuffer.bin"));
  CHECK(initBufferPool(bm, "testbuffer.bin", 3, RS_FIFO));
  printPoolContent(bm);
  
  CHECK(pinPage(bm, h, 0));
  printPoolContent(bm);
  CHECK(pinPage(bm, h, 0));
  sprintf(h->data, "%s-%i", "Page", h->pageNum);
  printPoolContent(bm);

  CHECK(markDirty(bm, h));
  printPoolContent(bm);

  CHECK(forcePage(bm, h));
  printPoolContent(bm);

  CHECK(unpinPage(bm,h));
  CHECK(unpinPage(bm,h));

  CHECK(shutdownBufferPool(bm));
  CHECK(destroyPageFile("testbuffer.bin"));

  free(bm);
  free(h);
}
