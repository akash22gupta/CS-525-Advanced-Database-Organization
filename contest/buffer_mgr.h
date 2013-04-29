#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <pthread.h>

// Replacement Strategies
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef short bool;
#define TRUE 1
#define FALSE 0

typedef struct BM_BufferPool {
  char *pageFile;
  int numPages;
  ReplacementStrategy strategy;
  void *mgmtData; // use this one to store the bookkeeping info your buffer 
                  // manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
  PageNumber pageNum;
  char *data;
} BM_PageHandle;

// Per Buffer Pool frame details
typedef struct BM_PageFrame {
    bool dirty;
    int fixCount;
    PageNumber pn;  // Owner of the frame.

    // Helps remove node in LRU faster,
    // When in case, we request of pin and the page is
    // found in pagetable, it is better to use same
    // page so as to avoid disk read. This need removal
    // of node from LRU, may be from mid of list.
    struct LRU_Node *lru_node;
    bool clockReplaceFlag;

    char data[PAGE_SIZE];
} BM_PageFrame;

// Per page table entries
#define BITS_PER_LEVEL 8   // Considering 4 byte int. 
                           // Each byte for 1 level of paging
#define MAX_PT_ENTRIES 256 // pow(2, BITS_PER_LEVEL)
typedef struct BM_PageTable {
    // If this refCount is 0, then we can delete 'this' page table.
    int refCount;

    // Entry can hold ptr to another page table
    // or ptr to page frame.
    void* entry[MAX_PT_ENTRIES];
} BM_PageTable;

// Strategy Related data structures
typedef struct LRU_Node {
  BM_PageFrame *frame;
  
  // List organized in a way that HEAD points to LRU frame
  // and TAIL points to MRU
  struct LRU_Node *next;
  struct LRU_Node *prev;
} LRU_Node;

typedef struct BM_StrategyInfo {
    // For FIFO
    int fifoLastFreeFrame;
    // For LRU
    LRU_Node *lru_head, *lru_tail;
    // For CLOCK
    int clockCurrentFrame;
} BM_StrategyInfo;

// Additional per BM details
typedef struct BM_Pool_MgmtData {
  SM_FileHandle fh;
  BM_PageFrame *pool;   // Heap mem = [numPages * sizeof(BM_PageFrame)] bytes
  BM_PageTable pt_head; // Keeps mapping of page number to page frame.
  int io_reads;
  int io_writes;
  BM_StrategyInfo stratData;

  // Gaurd's complete buffer manager
  pthread_mutex_t bm_mutex;
} BM_Pool_MgmtData;

// convenience macros
#define MAKE_POOL()				\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()		\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

#define MAKE_POOL_MGMTDATA()	\
  ((BM_Pool_MgmtData*) malloc (sizeof(BM_Pool_MgmtData)))

#define MAKE_LRU_NODE()         \
    ((LRU_Node*) malloc (sizeof(LRU_Node)))

#define MAKE_BUFFER_POOL(n)     \
    ((BM_PageFrame*) malloc (sizeof(BM_PageFrame) * n))

// Buffer Manager Interface - Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface - Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
