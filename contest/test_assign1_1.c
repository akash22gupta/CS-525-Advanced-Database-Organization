#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"

/* test output files */
#define TESTPF "test_pagefile.bin"

typedef short bool;
#define TRUE 1
#define FALSE 0

/* prototypes for test functions */
static void checkErrorCode (RC rc, char *message);
static RC testCreateOpenClose(void);
static RC testSinglePageContent(void);
static void myExit (int exitCode);

#define CHECK_RETURN_RC(rc) \
  CHECK(rc)
 
/* main function running all tests */
int
main (void)
{
  initStorageManager();

  checkErrorCode(testCreateOpenClose(), "creating, opening, and closing");
  checkErrorCode(testSinglePageContent(), "reading and writing single page content");

  myExit(0);
  return 0;
}

void
myExit (int exitCode)
{
  unlink(TESTPF);
  exit(exitCode);
}

/* check a return code. If it is not RC_OK then output a message, error description, and exit */
void
checkErrorCode (int rc, char *message)
{
  if (rc == RC_OK) {
    printf("TEST OK: %s\n", message);
    return;
  }
  printf("TEST FAILED: %s (%s: %i)\n", message, __FILE__, __LINE__);
  myExit(1);
}

/* fail with and error message */
#define FAIL(condition,message)			\
  do {						\
    if (!(condition)) {				\
      printf("%s\n", message);			\
      return -1;				\
    }						\
  } while(0)

/* Try to create, open, and close a page file */
RC
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  CHECK_RETURN_RC(createPageFile (TESTPF));
  
  CHECK_RETURN_RC(openPageFile (TESTPF, &fh));
  FAIL((strcmp(fh.fileName, TESTPF) == 0), "filename incorrect");
  FAIL((fh.totalNumPages == 1), "expected 1 page in new file");
  FAIL((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  CHECK_RETURN_RC(closePageFile (&fh));
  CHECK_RETURN_RC(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  FAIL((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  return RC_OK;
}

/* Try to create, open, and close a page file */
RC
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  CHECK_RETURN_RC(createPageFile (TESTPF));
  CHECK_RETURN_RC(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  CHECK_RETURN_RC(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    FAIL((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  CHECK_RETURN_RC(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  CHECK_RETURN_RC(readFirstBlock (&fh, ph));

  for (i=0; i < PAGE_SIZE; i++)
    FAIL((ph[i] == (i % 10) + '0'), "character in page read from disk not the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  CHECK_RETURN_RC(closePageFile (&fh));  
  CHECK_RETURN_RC(destroyPageFile (TESTPF));  
  
  free(ph);
  return RC_OK;
}
