#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

/* error codes for storage engine */
#define RC_SM_NOT_INIT 5
#define RC_MAX_FILE_HANDLE_OPEN 6
#define RC_FILE_CREATE_FAILED 7
#define RC_FILE_DESTROY_FAILED 8
#define RC_FILE_HANDLE_IN_USE 9
#define RC_FILE_CLOSE_FAILED 10
#define RC_READ_FAILED 11

/* error codes for Buffer Manager */
#define RC_FRAME_IN_USE 12
#define RC_BUFFER_POOL_FULL 13
#define RC_PAGE_NOT_PINNED 14
#define RC_HAVE_PINNED_PAGE 15

/* New error codes for Record manager */
#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205
#define RC_TOO_LARGE_SCHEMA 206
#define RC_TOO_LARGE_RECORD 207
#define RC_RM_INSERT_FAILED 208
#define RC_RM_DELETE_FAILED 209
#define RC_RM_UPDATE_FAILED 210

/* error codes for index manager */
#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303
#define RC_ORDER_TOO_HIGH_FOR_PAGE 304

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
