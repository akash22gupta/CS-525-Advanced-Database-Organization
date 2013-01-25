#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <stdlib.h>


#define RC_BM_ACCESS_PAGE_NOT_IN_MEM 100
#define RC_BM_UNPIN_PAGE_NOT_PINNED 101
#define RC_UNKOWN_REPLACEMENT_STRATEGY 102
#define RC_STRATEGY_CHOSE_FIXED_PAGE_FOR_REPLACEMENT 103
#define RC_BM_ALL_PAGES_FIXED 104

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
  void *stratData;
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

static RC loadPage(BM_BufferPool *bm, int frame, int pageNum);

// internal generic functions for replacement strategies
static RC initStrategyData (BM_BufferPool *const bm);
static RC choosePageFrame (BM_BufferPool *const bm, int *frameNum);
static RC shutdownStrategy (BM_BufferPool *const bm);

// FIFO
typedef struct IntCell
{
  int data;
  struct IntCell *next;
} IntCell;

typedef struct IntList
{
  IntCell *head;
  IntCell *tail;
  int length;
} IntList;

static IntList *lappend (IntList *list, int value);
#define NIL (IntList *) NULL
static IntList *ldelete (IntList *list, int pos);
#define llength(list) ((list == NIL) ? 0 : list->length)

static RC FIFO_initData (BM_BufferPool *const bm);
static RC FIFO_chooseNext (BM_BufferPool *const bm, int *frameNum);
static RC FIFO_shutdown (BM_BufferPool *const bm);

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

  initStrategyData(bm);

  return RC_OK;
}

RC 
shutdownBufferPool(BM_BufferPool *const bm)
{
  PoolData *pd;

  pd = (PoolData *) bm->mgmtData;


  // clean up bookkeeping info for strategy
  shutdownStrategy(bm);

  // flush and close page file
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
	    CHECK(writeBlock(pd->pageInFrame[i], pd->file, (SM_PageHandle) pd->frames[i]));
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
unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
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
  PoolData *pd;
  int f = 0;
  BM_PageHandle *oldPage;

  pd = (PoolData *) bm->mgmtData;  
  page->pageNum = pageNum;

  if (getFrame(bm,page,pd,&f) == RC_BM_ACCESS_PAGE_NOT_IN_MEM)
    {
      CHECK(choosePageFrame(bm, &f));
      if (pd->fixCount[f] > 0)
	return RC_STRATEGY_CHOSE_FIXED_PAGE_FOR_REPLACEMENT;
      
      // if frame is not empty flush if necessary and set frame for old page to empty
      if (pd->pageInFrame[f] != NO_PAGE)
	{
	  oldPage = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
	  oldPage->pageNum = pd->pageInFrame[f];
	  oldPage->data = pd->frames[f];

	  if (pd->dirty[f])
	      CHECK(forcePage(bm, oldPage));
	  pd->frameForPage[oldPage->pageNum] = NO_FRAME;
	  free(oldPage);
	}
      CHECK(loadPage(bm, f, pageNum));
    }
  
  pd->fixCount[f]++;
  page->data = pd->frames[f];

  return RC_OK;
}

RC 
loadPage(BM_BufferPool *bm, int frame, int pageNum)
{
  PoolData *pd;
  SM_PageHandle pageData;

  pd = (PoolData *) bm->mgmtData;
  pageData = (SM_PageHandle) pd->frames[frame];

  CHECK(readBlock(pageNum, pd->file, pageData));

  pd->pageInFrame[frame] = pageNum;
  pd->frameForPage[pageNum] = frame;
  pd->dirty[frame] = FALSE;
  pd->fixCount[frame] = 0;

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

// internal generic functions for replacement strategies
RC 
initStrategyData (BM_BufferPool *const bm)
{
  switch(bm->strategy)
    {
    case RS_FIFO:
      FIFO_initData(bm);
      break;
    default:
      return RC_UNKOWN_REPLACEMENT_STRATEGY;
    }

  return RC_OK;
}

RC 
choosePageFrame (BM_BufferPool *const bm, int *frameNum)
{
  switch(bm->strategy)
    {
    case RS_FIFO:
      FIFO_chooseNext(bm, frameNum);
      break;
    default:
      return RC_UNKOWN_REPLACEMENT_STRATEGY;
    }
  
  return RC_OK;
}

RC 
shutdownStrategy (BM_BufferPool *const bm)
{
  switch(bm->strategy)
    {
    case RS_FIFO:
      FIFO_shutdown(bm);
      break;
    default:
      return RC_UNKOWN_REPLACEMENT_STRATEGY;
    }
  
  return RC_OK;
}

// FIFO
RC 
FIFO_initData (BM_BufferPool *const bm)
{
  PoolData *pd;

  pd = (PoolData *) bm->mgmtData; 
  pd->stratData = NULL;

  return RC_OK;
}

RC 
FIFO_chooseNext (BM_BufferPool *const bm, int *frameNum)
{
  PoolData *pd;
  IntList *queue;
  IntCell *lc;
  int i;

  pd = (PoolData *) bm->mgmtData; 
  queue = (IntList *) pd->stratData;

  // if there are empty buffer frames left then choose the first empty one
  if (llength(queue) < bm->numPages)
    {
      for (i = 0; i < bm->numPages; i++)
	if (pd->pageInFrame[i] == NO_PAGE)
	  {
	    *frameNum = i;
	    pd->stratData = (void *) lappend(queue, i);
	    return RC_OK;
	  }
    }
  
  // walk through list until a page is found with fixCount 0
  for (i = 0, lc = queue->head; lc != NULL; i++, lc = lc->next)
    {
      int f = lc->data;
      if (pd->fixCount[f] == 0)
	{
	  *frameNum = f;
	  queue = ldelete(queue, i);
	  pd->stratData = (void *) lappend(queue, f);
	  return RC_OK;
	}
    }

  return RC_BM_ALL_PAGES_FIXED;
}

RC 
FIFO_shutdown (BM_BufferPool *const bm)
{
  PoolData *pd;
  IntList *queue;
  IntCell *lc, *f;

  pd = (PoolData *) bm->mgmtData; 
  queue = (IntList *) pd->stratData;
  
  if (queue == NIL)
    return RC_OK;

  for(lc = f = queue->head; lc != NULL; f = lc)
    {
      lc = lc -> next;
      free(f);
    }

  free(queue);

  return RC_OK;
}

// List functions
IntList *
lappend (IntList *list, int value)
{
  IntCell *newCell;

  newCell = (IntCell *) malloc(sizeof(IntCell));
  newCell->data = value;
  newCell->next = NULL;

  if (list == NIL)
    {
      IntList *new;
      new = (IntList *) malloc (sizeof(IntList));
      new->head = newCell;
      new->tail = newCell;
      new->length = 1;
      return new;
    }
  
  list->tail->next = newCell;
  list->tail = newCell;
  list->length++;

  return list;
}

IntList *
ldelete (IntList *list, int pos)
{
  IntCell *cur, *prev;
  int i;

  //TODO check that pos inside list
  
  if (pos == 0)
    {
      cur = list->head;
      list->head = cur->next;
      list->length--;

      if (list->length == 1)
	{
	  free(cur);
	  free(list);
	  return NIL;
	}

      return list;
    }

  prev = list->head;
  cur = prev->next;
  for (i = 1; i < pos; i++)
    {
      prev = cur;
      cur = cur->next;
    }

  prev->next = cur->next;
  if (cur == list->tail)
    list->tail = cur;
  
  free(cur);

  return list;
}

