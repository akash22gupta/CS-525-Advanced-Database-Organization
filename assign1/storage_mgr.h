#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;
#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

extern char *RC_message;

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  int fd;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *handle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *file);

/* print a message to standard out describing the error */
extern void printError (RC error);

#endif
