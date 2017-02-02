#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */

static void extra_test_cases(void);
static void additionalPageTesting(void);


/* main function running all tests */
int
main (void)
{
  testName = "";
  
  extra_test_cases();
  additionalPageTesting();


  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */



void
extra_test_cases(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);


    TEST_CHECK(createPageFile (TESTPF));
    TEST_CHECK(openPageFile (TESTPF, &fh));
    printf("created and opened file\n");

    // read first page into handle
    TEST_CHECK(readFirstBlock (&fh, ph));
    // the page should be empty (zero bytes)
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
    printf("first block was empty\n");

    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';
    TEST_CHECK(writeBlock (0, &fh, ph));
    printf("writing first block\n");

    // read back the page containing the string and check that it is correct
    TEST_CHECK(readBlock (0,&fh, ph));
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected");
    printf("reading first block\n");

    // change ph to be a string and write that one to disk for second block
       for (i=0; i < PAGE_SIZE; i++)
           ph[i] = (i % 10) + '0';
       TEST_CHECK(writeBlock (1, &fh, ph));
       printf("writing second block\n");

       // read back the page containing the string and check that it is correct for second block with readCurrentBlock
       TEST_CHECK(readCurrentBlock (&fh, ph));
       for (i=0; i < PAGE_SIZE; i++)
           ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected");
       printf("reading Current(second) block\n");


    // Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
       TEST_CHECK(appendEmptyBlock (&fh));
       ASSERT_TRUE((fh.totalNumPages == 3), "after appending to a new file, total number of pages should be 3");


     // read previous page into handle
        TEST_CHECK(readPreviousBlock (&fh, ph));
        for (i=0; i < PAGE_SIZE; i++)
           ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected");
          printf("reading Previous(second) block\n");


    // read Next page into handle
    TEST_CHECK(readNextBlock (&fh, ph));
    // the page should be empty (zero bytes)
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 0), "expected zero byte in newly appened page");
    printf("reading Next(third) block- the appended one\n");


    // change ph to be a string and write that one to disk on current page
    for (i=0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';
    TEST_CHECK(writeCurrentBlock (&fh, ph));
    printf("writing to the block which was appended\n");


    // If the file has less than numberOfPages pages then increase the size to numberOfPages by ensuring capacity to 5
    TEST_CHECK(ensureCapacity (5, &fh));
    printf("%d\n",fh.totalNumPages);
    ASSERT_TRUE((fh.totalNumPages == 5), "expect 5 pages after ensure capacity");

    //Now we will confirm if the pages are appended after ensure capacity
    ASSERT_TRUE((fh.curPagePos == 4), "After appending page position should be 4.(pointing to the last page 5)");

    // read last page into handle
    TEST_CHECK(readLastBlock (&fh, ph));
    // the page should be empty (zero bytes)
    for (i=0; i < PAGE_SIZE; i++)
        ASSERT_TRUE((ph[i] == 0), "expected zero byte in newly appened page");
    printf("reading the -last block- which was appended earlier. Expected empty\n");

  TEST_CHECK(destroyPageFile (TESTPF));
  free(ph);
  TEST_DONE();
}


void additionalPageTesting(void){
	  SM_FileHandle fh;
	  SM_PageHandle ph;
	  int i;

	  testName = "test second page content";

	  ph = (SM_PageHandle) malloc(PAGE_SIZE);

	  // create a new page file
	  TEST_CHECK(createPageFile (TESTPF));
	  TEST_CHECK(openPageFile (TESTPF, &fh));
	  printf("created and opened file\n");

	  //fh.totalNumPages=2;

	  for (i=0; i < PAGE_SIZE; i++)
	       ph[i] = (i % 10) + '0';
	  TEST_CHECK(writeBlock (0, &fh, ph));
	  printf("writing first block\n");

	  for (i=0; i < PAGE_SIZE; i++)
	      ph[i] = (i % 10) + '0';
	  TEST_CHECK(writeBlock (1, &fh, ph));
	  printf("writing second block\n");

	  // read back the page containing the string and check that it is correct
	   TEST_CHECK(readPreviousBlock (&fh, ph));
	   for (i=0; i < PAGE_SIZE; i++)
	     ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
	   printf("reading previous block\n");


	   // read back the page containing the string and check that it is correct
	   TEST_CHECK(readNextBlock (&fh, ph));
	   for (i=0; i < PAGE_SIZE; i++)
	     ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
	   printf("reading next block\n");


	  // read back the page containing the string and check that it is correct
	   TEST_CHECK(readLastBlock (&fh, ph));
	   for (i=0; i < PAGE_SIZE; i++)
	     ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
	   printf("reading last block\n");


	  TEST_CHECK(writeCurrentBlock (&fh, ph));
	   for (i=0; i < PAGE_SIZE; i++)
	  	 ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
	   printf("write current block\n");


	   TEST_CHECK(appendEmptyBlock (&fh));
	   printf("append empty block\n");

	   //printf("\n %d ", fh.totalNumPages);

	   TEST_CHECK(ensureCapacity (5, &fh));
	   	  ASSERT_TRUE((fh.totalNumPages==5),"checking total no. of pages is 5");
	   printf("ensure capacity \n");



	   TEST_CHECK(destroyPageFile (TESTPF));

	  TEST_DONE();
}
