#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <stdlib.h>


#define RC_BM_ACCESS_PAGE_NOT_IN_MEM 100
#define RC_BM_UNPIN_PAGE_NOT_PINNED 101

typedef char PageData[PAGE_SIZE];

#define NO_FRAME -1

#define MAX_NUM_PAGES 100000

typedef struct PoolData {
  SM_FileHandle *file;
  PageData *frames;
  int *pageInFrame;
  int *frameForPage;
  int *fixCount;
  bool *dirty;
  ReplacementStrategy strategy;
} PoolData;

#define CHECK(code)				\
  do {						\
    int rc_internal;				\
    rc_internal = (code);			\
    if (rc_internal != RC_OK)			\
      return rc_internal;			\
  } while (0)

// static functions
static RC getFrame(BM_BufferPool *bm, BM_PageHandle *page, PoolData *pd, int *frame);

#define GET_FRAME(bm,page,pd,frame)		\
  do {						\
   int rc_internal;				\
   rc_internal = getFrame(bm,page,pd,frame);	\
   if (rc_internal != RC_OK)			\
     return rc_internal;			\
  } while (0)


// functions
RC 
initBufferPool(BM_BufferPool *const bm , const char *pageFileName, const int numPages, ReplacementStrategy strategy)
{
  SM_FileHandle *h; 
  PoolData *pd;
  int i, rc;

  h = (SM_FileHandle *) malloc(sizeof(SM_FileHandle));
  pd = (PoolData *) malloc(sizeof(PoolData));

  CHECK(openPageFile((char *) pageFileName, h));

  pd->file = h;
  pd->frames = (PageData *) malloc(numPages * sizeof(PageData));
  pd->pageInFrame = (int *) malloc(numPages * sizeof(int));
  pd->frameForPage = (int *) malloc(MAX_NUM_PAGES * sizeof(int));
  pd->fixCount = (int *) malloc(numPages * sizeof(int));
  pd->dirty = (bool *) malloc(numPages * sizeof(bool));
  pd->strategy = strategy;

  for (i = 0; i < numPages; i++)
    {
      pd->pageInFrame[i] = NO_PAGE;
      pd->fixCount[i] = 0;
      pd->dirty[i] = FALSE;
    }

  for (i = 0; i < MAX_NUM_PAGES; i++)
    {
      pd->frameForPage[i] = NO_FRAME;
    }

  bm->pageFile = (char *) pageFileName;
  bm->strategy = strategy;
  bm->numPages = numPages;
  bm->mgmtData = pd;

  return RC_OK;
}

RC 
shutdownBufferPool(BM_BufferPool *const bm)
{
  PoolData *pd;

  pd = (PoolData *) bm->mgmtData;

  // close page file
  CHECK(forceFlushPool(bm));
  CHECK(closePageFile(pd->file));
  
  bm->mgmtData = NULL;
  
  // free pool
  free(pd->frames);
  free(pd->pageInFrame);
  free(pd->frameForPage);
  free(pd->fixCount);
  free(pd->dirty);
  free(pd);

  return RC_OK;
}

RC 
forceFlushPool(BM_BufferPool *const bm)
{
  PoolData *pd;
  int i;

  pd = (PoolData *) bm->mgmtData;

  for(i = 0; i < bm->numPages; i++)
    {
        if (pd->dirty[i] == TRUE)
	  {
	    CHECK(writeBlock(i, pd->file, (SM_PageHandle) pd->frames[i]));
	    pd->dirty[i] = FALSE;
	  }
    }

  return RC_OK;
}

RC 
markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  PoolData *pd;
  int f = 0;

  pd = (PoolData *) bm->mgmtData;  
  GET_FRAME(bm,page,pd,&f);
  pd->dirty[f] = TRUE;

  return RC_OK;
}

RC 
UnpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  PoolData *pd;
  int f = 0;

  pd = (PoolData *) bm->mgmtData;  
  GET_FRAME(bm,page,pd,&f);

  if (--(pd->fixCount[f]) < 0)
    {
      pd->fixCount[f] = 0;
      return RC_BM_UNPIN_PAGE_NOT_PINNED;
    }

  return RC_OK;
}

RC
getFrame(BM_BufferPool *bm, BM_PageHandle *page, PoolData *pd, int *frame)
{
  *frame = pd->frameForPage[page->pageNum];

  if (*frame == NO_FRAME)
    return RC_BM_ACCESS_PAGE_NOT_IN_MEM;
  return RC_OK;
}


RC 
forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
  PoolData *pd;
  int f = 0;

  pd = (PoolData *) bm->mgmtData;  
  GET_FRAME(bm,page,pd,&f);

  if (pd->dirty[f] == TRUE)
    {
      writeBlock(page->pageNum, pd->file, (SM_PageHandle) page->data);
      pd->dirty[f] = FALSE;
    }

  return RC_OK;
}

RC 
pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
  
  return RC_OK;
}

int *
getFrameContents (BM_BufferPool *const bm)
{
  PoolData *pd;

  pd = (PoolData *) bm->mgmtData;  
  return pd->pageInFrame;
}

bool *
getDirtyFlags (BM_BufferPool *const bm)
{
  PoolData *pd;

  pd = (PoolData *) bm->mgmtData;  
  return pd->dirty;
}

int *
getFixCounts (BM_BufferPool *const bm)
{
  PoolData *pd;

  pd = (PoolData *) bm->mgmtData;  
  return pd->fixCount;
}
