#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"

/* define boolean data type */
typedef int bool;
#define TRUE 1
#define FALSE 0

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void checkErrorCode (RC rc, char *message);
static bool testCreateOpenClose(void);
static bool testSinglePageContent(void);

#define CHECK_RETURN_RC(rc) \
  do { \
    RC intRC;	\
    intRC = (rc);     \
    if (intRC != RC_OK)	\
      return intRC;	\
  } while (0)
 
/* main function running all tests */
int
main (char **args)
{
  int returnCode;

  checkErrorCode(testCreateOpenClose(), "creating, opening, and closing tests failed");
  checkErrorCode(testSinglePageContent(), "reading and writing single page content failed");

  exit(0);
}

/* check a return code. If it is not RC_OK then output a message, error description, and exit */
void
checkErrorCode (int rc, char *message)
{
  if (rc == RC_OK)
    return;
  exit(1);
}

/* fail with and error message */
#define FAIL(condition,message)			\
  do { \
  printf(message); \
  return -1; \
  } while(0)

/* Try to create, open, and close a page file */
RC
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  CHECK_RETURN_RC(createPageFile (TESTPF));
  
  CHECK_RETURN_RC(openPageFile (TESTPF, &fh));
  FAIL((strcmp(fh.fileName, TESTPF) != 0), "filename incorrect");
  FAIL((fh.totalNumPages == 1), "expected 1 page in new file");
  FAIL((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  CHECK_RETURN_RC(closePageFile (&fh));
  CHECK_RETURN_RC(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  FAIL((openPageFile(TESTPF, &fh) == RC_OK), "opening non-existing file should return an error.");

  return RC_OK;
}

/* Try to create, open, and close a page file */
RC
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  ph = malloc(PAGE_SIZE);

  // create a new page file
  CHECK_RETURN_RC(createPageFile (TESTPF));
  CHECK_RETURN_RC(openPageFile (TESTPF, &fh));
  
  // read first page into handle
  CHECK_RETURN_RC(readFirstBlock (&fh, &ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    FAIL((ph[i] != '\0'), "expected zero byte in first page of freshly initialized page");
      
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  CHECK_RETURN_RC(writeCurrentBlock (&fh, &ph));

  // read back the page containing the string and check that it is correct
  CHECK_RETURN_RC(readFirstBlock (&fh, &ph));
  for (i=0; i < PAGE_SIZE; i++)
    FAIL((ph[i] == (i % 10) + '0'), "character in page read from disk not the one we expected.");

  // destroy new page file
  CHECK_RETURN_RC(destroyPageFile (TESTPF));  
  
  return RC_OK;
}
