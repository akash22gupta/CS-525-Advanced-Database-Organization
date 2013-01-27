#include "buffer_mgr.h"

#include <stdio.h>

// local functions
static void printStrat (BM_BufferPool *const bm);

// external functions
void 
printPoolContent (BM_BufferPool *const bm)
{
  int *frameContent;
  bool *dirty;
  int *fixCount;
  int i;

  frameContent = getFrameContents(bm);
  dirty = getDirtyFlags(bm);
  fixCount = getFixCounts(bm);

  printf("{");
  printStrat(bm);
  printf(" %i}: ", bm->numPages); 
  
  for (i = 0; i < bm->numPages; i++)
      printf("%s[%i%s%i]", ((i == 0) ? "" : ",") , frameContent[i], (dirty[i] ? "x": " "), fixCount[i]);
  printf("\n");
}

void
printPageContent (BM_PageHandle *const page)
{
  int i;

  printf("[Page %i]\n", page->pageNum);

  for (int i = 1; i <= PAGE_SIZE; i++)
    printf("%02X%s%s", page->data[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n"); 
  printf("\n\n");
}

void
printStrat (BM_BufferPool *const bm)
{
  switch (bm->strategy)
    {
    case RS_FIFO:
      printf("FIFO");
      break;
    case RS_LRU:
      printf("LRU");
      break;
    case RS_CLOCK:
      printf("CLOCK");
      break;
    case RS_LFU:
      printf("LFU");
      break;
    case RS_LRU_K:
      printf("LRU-K");
      break;
    default:
      printf("%i", bm->strategy);
      break;
    }
}
