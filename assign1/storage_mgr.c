#include "storage_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sys/types.h> 
#include <sys/stat.h> 
#include <errno.h>
#include <fcntl.h> 
#include <unistd.h> 

char *RC_message = NULL;

/* management structure for page files */
static char *buffer = NULL;

#define CHECK_FD(fd,emessage) \
  do { \
  if (fd < 0 ) { \
    printf( "Error opening file: %s\n", strerror(errno));	\
    RC_message = emessage; \
    return RC_FILE_NOT_FOUND; \
  } \
  } while (0)

#define CHECK_SEEK(seek) \
  do { \
  int spos; \
  spos = (seek); \
  if (spos < 0 ) { \
    printf( "Error seeking in file: %s\n", strerror(errno));	\
    return RC_FILE_NOT_FOUND; \
  } \
  } while (0)

#define CHECK_READ(readcmd) \
  do { \
  int bread; \
  bread = (readcmd); \
  if (bread < 0 ) { \
    printf( "Error seeking in file: %s\n", strerror(errno));	\
    return RC_FILE_NOT_FOUND; \
  } \
  } while (0)

#define CHECK_WRITE(writecmd) \
  do { \
  int bwrite; \
  bwrite = (writecmd); \
  if (bwrite < 0 ) { \
    printf( "Error seeking in file: %s\n", strerror(errno));	\
    return RC_FILE_NOT_FOUND; \
  } \
  } while (0)


void
initStorageManager (void)
{
  buffer = malloc(PAGE_SIZE);
  printf("initialized buffer for storage manager\n");
}

RC 
createPageFile (char *fileName)
{
  int fd;

  fd = open(fileName, O_WRONLY | O_CREAT | O_SYNC, 0644);
  CHECK_FD(fd,"could not create file");

  // create content of header page (number of total pages = 1) and write to file
  memset((void *) buffer, 0, PAGE_SIZE);
  *((int *) buffer) = 1;

  CHECK_WRITE(write (fd, (void *) buffer, PAGE_SIZE));

  // create dummy first data page and write to file
  memset((void *) buffer, 0, PAGE_SIZE);
  CHECK_WRITE(write(fd, (void *) buffer, PAGE_SIZE));

  // close file
  close(fd);

  return RC_OK;
}

RC 
openPageFile (char *fileName, SM_FileHandle *fHandle)
{
  int fd;

  fd = open(fileName, O_RDWR | O_SYNC);
  CHECK_FD(fd, "could not open page file");
	    
  fHandle->fileName = fileName;

  // read header page and set handle fields
  CHECK_READ(read(fd, (void *) buffer, PAGE_SIZE));

  fHandle->totalNumPages = *((int *) buffer);
  fHandle->curPagePos = 0;
  fHandle->fd = fd;

  printf("opened file successful\n");

  return RC_OK;
}

RC 
closePageFile (SM_FileHandle *fHandle)
{ 
  int spos;

  // write header
  CHECK_SEEK(lseek(fHandle->fd, 0, SEEK_SET));
  CHECK_READ(read(fHandle->fd, (void *) buffer, PAGE_SIZE));

  *((int *) buffer) = fHandle->totalNumPages;
  CHECK_SEEK(lseek(fHandle->fd, 0, SEEK_SET));
  CHECK_WRITE(write(fHandle->fd, (void *) buffer, PAGE_SIZE));

  close(fHandle->fd);
  CHECK_FD(fHandle->fd, "error closing file");
  return RC_OK;
}

RC 
destroyPageFile (char *fileName)
{
  int rt;

  rt = unlink(fileName);
  CHECK_FD(rt, "could not delete file");

  return RC_OK;
}

/* reading blocks from disc */
RC 
readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_SEEK(lseek(fHandle->fd, (PAGE_SIZE * (pageNum + 1)) - 1, SEEK_SET));
  CHECK_READ(read(fHandle->fd, (void *) memPage, PAGE_SIZE));
  fHandle->curPagePos = pageNum + 1;

  return RC_OK;
}

int
getBlockPos (SM_FileHandle *fHandle)
{
  return fHandle->curPagePos;
}

RC
readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_SEEK(lseek(fHandle->fd, PAGE_SIZE - 1, SEEK_SET));
  CHECK_READ(read(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  fHandle->curPagePos = 1;
  
  return RC_OK;
}

RC
readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  if (fHandle->curPagePos == 0)
    return RC_READ_NON_EXISTING_PAGE;

  CHECK_SEEK(lseek(fHandle->fd, -PAGE_SIZE, SEEK_CUR));
  CHECK_READ(read(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  fHandle->curPagePos--;

  return RC_OK;
}

RC
readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_READ(read(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  
  return RC_OK;
}

RC 
readNextBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_SEEK(lseek(fHandle->fd, -PAGE_SIZE, SEEK_CUR));
  CHECK_READ(read(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  fHandle->curPagePos++;

  return RC_OK;
}

RC 
readLastBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_SEEK(lseek(fHandle->fd, (fHandle->totalNumPages * PAGE_SIZE) -1, SEEK_SET));
  CHECK_READ(read(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  fHandle->curPagePos = fHandle->totalNumPages;

  return RC_OK;
}

/* writing blocks to a page file */
RC
writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_SEEK(lseek(fHandle->fd, ((pageNum + 1) * PAGE_SIZE) - 1, SEEK_SET));
  CHECK_WRITE(write(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  fHandle->curPagePos = pageNum + 1;

  return RC_OK;
}

RC 
writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle *memPage)
{
  CHECK_WRITE(write(fHandle->fd, (void *) *memPage, PAGE_SIZE));
  fHandle->curPagePos++;

  return RC_OK;
}

RC 
appendEmptyBlock (SM_FileHandle *fHandle)
{
  CHECK_SEEK(lseek(fHandle->fd, ((fHandle->totalNumPages + 1) * PAGE_SIZE) - 1, SEEK_SET));
  memset(buffer, 0, PAGE_SIZE);
  CHECK_WRITE(write(fHandle->fd, (void *) buffer, PAGE_SIZE));
  fHandle->curPagePos = ++(fHandle->totalNumPages);

  return RC_OK;
}

RC 
ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
  while(fHandle->totalNumPages < numberOfPages)
    appendEmptyBlock(fHandle);
  return RC_OK;
}

/* print a message to standard out describing the error */
void 
printError (RC error)
{
  if (RC_message != NULL)
    printf("EC (%i), \"%s\"\n", error, RC_message);
  else
    printf("EC (%i)\n", error);
}
