#include <page_table.h>
#include <string.h>
#include <assert.h>

/*
 * Page to frame mapping
 *
 * This concept is very similar to how OS maps virtual
 * pages to physical page frames in RAM.
 *
 * We use similar concept. We use 4 level of page tables,
 * Following is bit map of integer that represents page number.
 * | Level4 8bit | Level3 8bit | Level2 8bit | Level1 8bit | 
 *
 * We initially have 2^16 integers pointed to by pt_head;
 * Finding frame with given offset: BM_PageFrame* findPageFrame(PageNumber pn);
 * -------------------------------
 * 1) Read Level1 offset and use it as offset to pt_head.
 *    Value in pt_head at this offset is ptr to level2 page table.
 *
 * 2) Read Level2 offset and use it as offset to pt_head_level2.
 *    Value in pt_head_level2 at this offset is ptr to level3 page table.
 *
 * 3) Read Level3 offset and use it as offset to pt_head_level3.
 *    Value in pt_head_level3 at this offset is ptr to level3 page table.
 *
 * 4) Read Level4 offset and use it as offset to pt_head_level4.
 *    Value in pt_head_level4 at this offset is ptr to page frame.
 *
 * Add entries in page table: setPageFrame(PageNumber pn, BM_PageFrame *frame);
 * ----------------------------
 *  1) Use level1 offset from pn.
 *     check if value at offset in pt_head is NULL, then create new page table.
 *  2) Use level2 offset from pn.
 *     check if value at offset in pt_head is NULL, then create new page table.
 *  3) Use level3 offset from pn.
 *     check if value at offset in pt_head is NULL, then create new page table.
 *  4) Use level4 offset from pn.
 *     Give error if value at this offset is not null;
 *     check if value at offset is NULL, then store *frame here.
 *
 */

#define OFFSET_OF_LEVEL(pn, lvl) ( (pn>>((4-lvl)*BITS_PER_LEVEL)) & 0x000000FF );

#define MAKE_PAGE_TABLE()				\
  ((BM_PageTable *) malloc (sizeof(BM_PageTable)))

// Not a interface
static void setPageFrameRecursive(BM_PageTable *pt, PageNumber pn,
                         BM_PageFrame *frame, int startlevel);
static BM_PageFrame* findPageFrameRecursive(BM_PageTable *pt, PageNumber pn,
                                   int startlevel);
static void resetPageFrameRecursive(BM_PageTable *pt, PageNumber pn, int startlevel);

// Initialize complete page table to 0
void initPageTable(BM_PageTable *pt)
{
  memset((void*)pt, 0, sizeof(BM_PageTable));
}

// Map: Set page with a frame
void setPageFrame(BM_PageTable *pt, PageNumber pn, BM_PageFrame *frame)
{ setPageFrameRecursive(pt, pn, frame, 1); }
void setPageFrameRecursive(BM_PageTable *pt, PageNumber pn,
                  BM_PageFrame *frame, int startlevel)
{
  PageNumber offset= OFFSET_OF_LEVEL(pn, startlevel);
  BM_PageTable* pt_ptr= pt->entry[offset];

  if (startlevel==4)
  {
    // Map it now
    if (pt_ptr==NULL)
    {
      pt->entry[offset]= (void*) frame;
      pt->refCount++;
    }
    else
      assert(!"We should not hit here"); 
      // We should reset entries when removing mapping properly.
    return;
  }

  if (pt_ptr==NULL)
  {
    pt_ptr= MAKE_PAGE_TABLE();
    initPageTable(pt_ptr);
    pt->entry[offset]= pt_ptr;
    pt->refCount++;
  }

  setPageFrameRecursive(pt_ptr, pn, frame, startlevel+1);
}

// Finding frame with given offset
BM_PageFrame* findPageFrame(BM_PageTable *pt, PageNumber pn)
{ return findPageFrameRecursive(pt, pn, 1); }
BM_PageFrame* findPageFrameRecursive(BM_PageTable *pt, PageNumber pn, int startlevel)
{
  PageNumber offset= OFFSET_OF_LEVEL(pn, startlevel);
  BM_PageTable* pt_ptr= pt->entry[offset];

  // At level 4 pt_ptr is actually frame pointer
  if (startlevel==4)
    return (BM_PageFrame*) pt_ptr;

  if (pt_ptr==NULL)
    return NULL;

  return findPageFrameRecursive(pt_ptr, pn, startlevel+1);
}

// Removes the mapping of page and frame.
void resetPageFrame(BM_PageTable *pt, PageNumber pn)
{ resetPageFrameRecursive(pt, pn, 1); }
void resetPageFrameRecursive(BM_PageTable *pt, PageNumber pn, int startlevel)
{
  short offset= OFFSET_OF_LEVEL(pn, startlevel);
  BM_PageTable* pt_ptr= pt->entry[offset];

  // You are at the last level of page table, which points
  // to page frame.
  if (startlevel==4)
  {
    // Remove mapping and reduce refCount.
    if (pt_ptr!=NULL)
    {
      pt->entry[offset]= NULL;
      pt->refCount--;
      return;
    }
  }

  // There is no frame associated with pn.
  if (pt_ptr==NULL)
    return;

  // Recursively try and find frame.
  resetPageFrameRecursive(pt_ptr, pn, startlevel+1);

  // Remove page table if no entries are in use.
  if (pt_ptr->refCount==0)
  {
      free (pt_ptr);
      pt->entry[offset]= NULL;
      pt->refCount--;
  }

  return;
}
