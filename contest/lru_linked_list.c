#include <stdlib.h>
#include <assert.h>
#include "lru_linked_list.h"

// Just to make code look clean
#define HEAD (si->lru_head)
#define TAIL (si->lru_tail)

// Added at TAIL of list, representing
// most recently used frame.
void appendMRUFrame(BM_StrategyInfo *si, BM_PageFrame *pf)
{
  LRU_Node *node; 
  node= MAKE_LRU_NODE();
  node->next = NULL;
  node->prev = NULL;
  node->frame= pf;
  pf->lru_node= node;

  if(si->lru_head == NULL)
    HEAD= TAIL= node;
  else if(HEAD == TAIL)
  { 
    TAIL= node;
    HEAD->next= TAIL;
    TAIL->prev= HEAD;
  } else 
  {
    TAIL->next= node;
    node= TAIL->next;
    node->prev= TAIL;
    TAIL= node;
  } 
}

// Returns least resently used frame
// from HEAD of the list.
BM_PageFrame* retriveLRUFrame(BM_StrategyInfo *si)
{
  LRU_Node *temp;
  BM_PageFrame *pf;

  if(HEAD == NULL)
    return NULL;

  // Read frame and Remove LRU node
  pf= HEAD->frame;
  pf->lru_node= NULL;
  temp= HEAD->next;
  free(HEAD);
  if (temp)
    temp->prev = NULL;
  else
    TAIL= NULL;
  HEAD= temp;
  
  return pf;
}

// Remove a entry from LRU list
// representing frame/page is in use now.
void reuseLRUFrame(BM_StrategyInfo *si, BM_PageFrame *pf)
{
  LRU_Node *node;
  LRU_Node *temp;

  assert(pf->lru_node->frame == pf);
  node= pf->lru_node;
  pf->lru_node= NULL;

  if (node == HEAD && node == TAIL)
  {
    free(node);
    HEAD= TAIL= NULL;
  } else if(node == HEAD)
  {
    HEAD= HEAD->next;
    HEAD->prev= NULL;
  } else if(node == TAIL)
  {
    TAIL= node->prev;
    TAIL->next= NULL;
  } else
  {
    temp= node->prev;
    temp->next= node->next;
    (temp->next)->prev= temp;
  }

  free(node);
}

// Remove all nodes
void cleanLRUlist(BM_StrategyInfo *si)
{
  LRU_Node *temp;
  while (HEAD != NULL)
  {
    temp= HEAD->next;
    free(HEAD);
    HEAD= temp;
  }
  HEAD= TAIL= NULL;
} 
